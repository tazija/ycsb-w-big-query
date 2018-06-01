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
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cassandra 2.x CQL client.
 *
 * See {@code cassandra2/README.md} for details.
 *
 * @author cmatser
 */
public class CassandraCQLClientCustom extends CassandraCQLClient {

  private static Logger logger = LoggerFactory.getLogger(CassandraCQLClientCustom.class);


  private static ConcurrentMap<Set<String>, PreparedStatement> insertStmts =
      new ConcurrentHashMap<>();
  private static ConcurrentMap<Set<String>, PreparedStatement> updateStmts =
      new ConcurrentHashMap<>();
  private static ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
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
}
