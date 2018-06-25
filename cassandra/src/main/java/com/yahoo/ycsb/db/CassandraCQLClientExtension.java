/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 * Submitted by Chrisjan Matser on 10/11/2010.
 */
package com.yahoo.ycsb.db;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClientExtension extends CassandraCQLClient {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClientExtension.class);

  private static AtomicReference<PreparedStatement> readAllStmt =
      new AtomicReference<PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> readStmts =
      new ConcurrentHashMap<Set<String>, PreparedStatement>();
  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts =
      new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts =
      new ConcurrentHashMap<>();
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ANY;
  private static boolean trace = false;

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values, Map<String, Type> model) {
    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = preparedStatementOnDemand(table, fields);

      logsForDebug(stmt, key, values);

      // Add key
      BoundStatement boundStmt = stmt.bind().setString(0, key);

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      for (int i = 1; i < vars.size(); i++) {
        switch (model.get(vars.getName(i))) {
        case DOUBLE:
          boundStmt.setDouble(i, ((NumericByteIterator)values.get(vars.getName(i))).getDouble());
          break;
        default:
          boundStmt.setString(i, values.get(vars.getName(i)).toString());
        }
      }

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error inserting key: {}", key).getMessage(), e.getCause());
    }

    return Status.ERROR;
  }

  private PreparedStatement preparedStatementOnDemand(String table, Set<String> fields) {
    PreparedStatement stmt = insertStmts.get(fields);

    // Prepare statement on demand
    if (stmt == null) {
      Insert insertStmt = QueryBuilder.insertInto(table);
      // Add key
      insertStmt.value(YCSB_KEY, QueryBuilder.bindMarker());
      // Add fields
      for (String field : fields) {
        insertStmt.value(field, QueryBuilder.bindMarker());
      }

      stmt = session.prepare(insertStmt);
      stmt.setConsistencyLevel(writeConsistencyLevel);
      if (trace) {
        stmt.enableTracing();
      }

      PreparedStatement prevStmt = insertStmts.putIfAbsent(new HashSet<>(fields), stmt);
      if (prevStmt != null) {
        stmt = prevStmt;
      }
    }

    return stmt;
  }

  private void logsForDebug(PreparedStatement stmt, String key, Map<String, ByteIterator> values) {
    if (logger.isDebugEnabled()) {
      logger.debug(stmt.getQueryString());
      logger.debug("key = {}", key);
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        logger.debug("{} = {}", entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
   * record with the specified record key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values, Map<String, Type> model) {

    try {
      Set<String> fields = values.keySet();
      PreparedStatement stmt = updateStmts.get(fields);

      // Prepare statement on demand
      if (stmt == null) {
        Update updateStmt = QueryBuilder.update(table);

        // Add fields
        for (String field : fields) {
          updateStmt.with(QueryBuilder.set(field, QueryBuilder.bindMarker()));
        }

        // Add key
        updateStmt.where(QueryBuilder.eq(YCSB_KEY, QueryBuilder.bindMarker()));

        stmt = session.prepare(updateStmt);
        stmt.setConsistencyLevel(writeConsistencyLevel);
        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement prevStmt = updateStmts.putIfAbsent(new HashSet<>(fields), stmt);
        if (prevStmt != null) {
          stmt = prevStmt;
        }
      }

      logsForDebug(stmt, key, values);

      // Add fields
      ColumnDefinitions vars = stmt.getVariables();
      BoundStatement boundStmt = stmt.bind();
      for (int i = 0; i < vars.size() - 1; i++) {
        switch (model.get(vars.getName(i))) {
        case DOUBLE:
          boundStmt.setDouble(i, ((NumericByteIterator)values.get(vars.getName(i))).getDouble());
          break;
        case LONG:
          boundStmt.setLong(i, ((NumericByteIterator)values.get(vars.getName(i))).getLong());
          break;
        default:
          boundStmt.setString(i, values.get(vars.getName(i)).toString());
        }
      }

      // Add key
      boundStmt.setString(vars.size() - 1, key);

      session.execute(boundStmt);

      return Status.OK;
    } catch (Exception e) {
      logger.error(MessageFormatter.format("Error updating key: {}", key).getMessage(), e);
    }

    return Status.ERROR;
  }

  /**
   * Perform a query type 1 execution - filter with OFFSET and LIMIT for a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param filterfield The field to filter records by.
   * @param filtervalue The value to use in 'WHERE' clause.
   * @param offset    Read offset.
   * @param recordcount The number of records to read.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status query1(String table, String filterfield, String filtervalue, int offset, int recordcount,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    fields = new HashSet<>();
    fields.add("id");
    try {
      PreparedStatement stmt = readStmts.get(fields);

      if (stmt == null) {
        Select.Builder selectBuilder;

        selectBuilder = QueryBuilder.select();
        for (String col : fields) {
          ((Select.Selection) selectBuilder).column(col);
        }

        stmt = session.prepare(selectBuilder.from(table)
            .where(QueryBuilder.eq(filterfield, QueryBuilder.bindMarker()))
            .limit(recordcount));
        stmt.setConsistencyLevel(ConsistencyLevel.ONE);

        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement preparedStatement = readStmts.putIfAbsent(new HashSet<>(fields), stmt);

        if (preparedStatement != null) {
          stmt = preparedStatement;
        }
      }

      ResultSet rs = session.execute(stmt.bind(filtervalue));

      List<Row> rows = StreamSupport.stream(rs.spliterator(), false)
          .skip(offset)
          .collect(Collectors.toList());

      return Status.OK;

    } catch (Exception e) {
      System.out.println(e.getMessage());
      logger.error(MessageFormatter.format("Error reading key: {}", filterfield).getMessage(), e);
      return Status.ERROR;
    }
  }

  /**
   * Perform a query type 2 execution - JOIN​ with GROUP BY and ORDER BYfor a set of records in the database.
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param filterfield1 The first field to filter records by.
   * @param filtervalue1 The first value to use in 'WHERE' clause.
   * @param filterfield2 The second field to filter records by.
   * @param filtervalue2 The second value to use in 'WHERE' clause.
   * @param fields The list of fields to read, or null for all of them.
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status query2(
      String table, String filterfield1, String filtervalue1, String filterfield2, String filtervalue2,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result
  ) {
    try {
      PreparedStatement stmt = (fields == null) ? readAllStmt.get() : readStmts.get(fields);

      if (stmt == null) {
        stmt = session.prepare("SELECT zip, month, SUM(sale_price) FROM " + table +
            " WHERE zip = :zip AND  month = :month ;");

        stmt.setConsistencyLevel(ConsistencyLevel.ONE);

        if (trace) {
          stmt.enableTracing();
        }

        PreparedStatement preparedStatement = (fields == null) ?
            readAllStmt.getAndSet(stmt) :
            readStmts.putIfAbsent(new HashSet<>(fields), stmt);

        if (preparedStatement != null) {
          stmt = preparedStatement;
        }
      }

      ResultSet rs = session.execute(stmt.bind().setString("zip", filtervalue1)
          .setString("month", filtervalue2));

      if (rs.isExhausted()) {
        return Status.NOT_FOUND;
      }

      return Status.OK;

    } catch (Exception e) {
      System.out.println(e.getMessage());
      logger.error(MessageFormatter.format("Error reading key: {}, {}", filterfield1, filterfield2).getMessage(), e);
      return Status.ERROR;
    }
  }
}
