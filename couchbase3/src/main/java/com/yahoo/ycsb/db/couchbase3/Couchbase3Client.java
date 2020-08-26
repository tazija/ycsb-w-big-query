package com.yahoo.ycsb.db.couchbase3;

import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Utils.getId;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.query.QueryIndexManager;
import com.google.common.collect.ImmutableSet;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.function.Function;

/**
 * A class that wraps the 2.x Couchbase SDK to be used with YCSB.
 */
public class Couchbase3Client extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(Couchbase3Client.class);

  private static final TypeRef<Map<String, ByteIterator>> RESULT_TYPE =
      new TypeRef<Map<String, ByteIterator>>() {
      };

  private static final Object LOCK = new Object();
  private static final int TRIES = 60;

  private Couchbase3Operations operations;

  @Override
  public void init() throws DBException {
    try {
      synchronized (LOCK) {
        operations = new Couchbase3Operations(getProperties());
        if (!operations.isKv()) {
          QueryIndexManager indexes = operations.getCluster().queryIndexes();
          indexes.createPrimaryIndex(
              operations.getBucketName(),
              createPrimaryQueryIndexOptions().ignoreIfExists(true)
          );
        }
      }
    } catch (Exception exception) {
      throw new DBException("Can't connect to Couchbase", exception);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String docId = getId(table, key);
    try {
      if (fields.size() > 16) {
        fields = null;
      }
      result.putAll(operations.get(docId, fields).contentAs(RESULT_TYPE));
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("read() failed docId {}", docId);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    String docId = getId(table, key);
    try {
      operations.update(docId, values);
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("update() failed docId {}", docId);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values, Map<String, Type> model) {
    // todo: implement
    // return update(table, key, values);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    String docId = getId(table, key);
    try {
      for (int i = 0; i < TRIES; i++) {
        try {
          operations.insert(docId, values);
          return Status.OK;
        } catch (TemporaryFailureException exception) {
          backoff(exception);
        }
      }
      LOGGER.error("insert() failed docId {}", docId);
      return Status.ERROR;
    } catch (Exception exception) {
      LOGGER.error("insert() failed docId {}", docId);
      return Status.ERROR;
    }
  }

  private void backoff(Exception exception) {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          format("Interrupted while backoff upon %s", exception.getMessage()));
    }
  }

  @Override
  public Status insert(String table,
                       String key,
                       Map<String, ByteIterator> values,
                       Map<String, Type> model) {
    // todo implement
    // insert(table, key, values);
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    String docId = getId(table, key);
    try {
      operations.remove(docId);
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("delete() failed docId {}", docId);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startKey, int docCount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String docId = getId(table, startKey);
    try {
      StringBuilder query = new StringBuilder("SELECT ");
      // if no fields to fetch specified select indexed field only
      if (fields == null || fields.isEmpty()) {
        query.append("meta().id");
      } else {
        query.append(fields(fields, true));
      }
      query.append(" FROM `").append(operations.getBucketName()).append("`");
      query.append(" WHERE meta().id >= '$1' LIMIT $2");
      JsonArray parameters = JsonArray.from(docId, docCount);
      query(query.toString(), parameters, result, object -> object.getObject(operations.getBucketName()));
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("scan() failed start docId {} docCount {}", docId, docCount, exception);
      return Status.ERROR;
    }
  }

  /**
   * CREATE PRIMARY INDEX ON `bucket` USING GSI;
   * CREATE INDEX address_country ON `bucket`(`address`.`country`) USING GSI;
   * SELECT * FROM system:indexes;
   */
  @Override
  public Status query1(String table,
                       String filterField, String filterValue,
                       int offset, int recordCount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
    try {
      StringBuilder query = new StringBuilder("SELECT ");
      // if no fields to fetch specified select indexed field only
      if (fields == null || fields.isEmpty()) {
        query.append("meta().id");
      } else {
        query.append(fields(fields, true));
      }
      query.append(" FROM `").append(operations.getBucketName()).append("`");
      query.append(" WHERE ").append(filterField).append(" = $1");
      query.append(" OFFSET $2 LIMIT $3");
      JsonArray parameters = JsonArray.from(filterValue, offset, recordCount);
      query(query.toString(), parameters, result, object -> object);
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("query1() failed filterField {} filterField {} offset {} recordCount {}",
          filterField, filterValue, offset, recordCount, exception);
      return Status.ERROR;
    }
  }

  /**
   * CREATE INDEX `address_zip_month_order_list_sale_price` ON `bucket`(address.zip, month, order_list, sale_price)
   * USING GSI;
   * SELECT * FROM system:indexes;
   */
  @Override
  public Status query2(String table,
                       String filterField1, String filterValue1,
                       String filterField2, String filterValue2,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      @SuppressWarnings("StringBufferReplaceableByString")
      StringBuilder query = new StringBuilder("SELECT ");
      query.append(fields(ImmutableSet.of(
          "c2." + filterField1, "o2." + filterField2, "SUM(o2.sale_price) as sale_price"), false));
      query.append(" FROM `").append(operations.getBucketName()).append("` c2");
      query.append(" INNER JOIN `").append(operations.getBucketName()).append("` o2");
      query.append(" ON KEYS c2.order_list WHERE c2.").append(filterField1).append(" = $1");
      query.append(" AND o2.").append(filterField2).append(" = $2");
      query.append(" GROUP BY ").append(fields(ImmutableSet.of("c2." + filterField1, "o2." + filterField2), false));
      query.append(" ORDER BY SUM(o2.sale_price)");
      query(query.toString(), JsonArray.from(filterValue1, filterValue2), result, object -> object);
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("query2() failed filterValue1 {} filterValue2 {}", filterValue1, filterValue2, exception);
      return Status.ERROR;
    }
  }

  @Override
  public Status query3(String table,
                       String filterField1, String filterValue1,
                       String filterField2, String filterValue2,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("query3() is not implemented");
  }

  private void query(String query,
                     JsonArray parameters,
                     Vector<HashMap<String, ByteIterator>> result,
                     Function<JsonObject, JsonObject> resultMapper) {
    List<JsonObject> rows = operations.query(query, parameters).rowsAsObject();
    result.addAll(
        rows.stream()
            .map(resultMapper)
            .filter(Objects::nonNull)
            .map(object -> object.toMap().entrySet().stream()
                .collect(toMap(
                    Map.Entry::getKey,
                    entry -> (ByteIterator) new StringByteIterator(entry.getValue().toString()),
                    (value1, value2) -> value1,
                    HashMap::new)))
            .collect(toList())
    );
  }

  private static String fields(Set<String> fields, boolean quote) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (Iterator<String> iterator = fields.iterator(); iterator.hasNext(); ) {
      String field = iterator.next();
      if (quote) {
        builder.append("`");
      }
      builder.append(field);
      if (quote) {
        builder.append("`");
      }
      if (iterator.hasNext()) {
        builder.append(",");
      }
    }
    return builder.toString();
  }
}