package com.yahoo.ycsb.db.couchbase3;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions.createPrimaryQueryIndexOptions;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.BUCKET;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.DOCUMENT_EXPIRY;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.HOST;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.KV;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.PASSWORD;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.PERSIST_TO;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.REPLICATE_TO;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.UPSERT;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.USERNAME;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Utils.getId;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Utils.parsePersistTo;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Utils.parseReplicateTo;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JacksonJsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonValueModule;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A class that wraps the 2.x Couchbase SDK to be used with YCSB.
 *
 * <p> The following options can be passed when using this database client to override the defaults.
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server.</li>
 * <li><b>couchbase.bucket=default</b> The bucket name to use.</li>
 * <li><b>couchbase.password=</b> The password of the bucket.</li>
 * <li><b>couchbase.syncMutationResponse=true</b> If mutations should wait for the response to complete.</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace.</li>
 * <li><b>couchbase.kv=true</b> If set to false, mutation operations will also be performed through N1QL.</li>
 * <li><b>couchbase.documentExpiry=0</b> Document Expiry is the amount of time until a document expires in Couchbase
 * .</li>
 * </ul>
 */
public class Couchbase3Client extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(Couchbase3Client.class);

  private static final TypeRef<Map<String, ByteIterator>> RESULT_TYPE =
      new TypeRef<Map<String, ByteIterator>>() {
      };
  private static final JacksonJsonSerializer SERIALIZER = JacksonJsonSerializer.create(
      new ObjectMapper()
          .registerModule(new JsonValueModule())
          .registerModule(new Couchbase3JacksonModule())
  );

  private static Object LOCK = new Object();
  private static final int TRIES = 60;
  private ClusterEnvironment ENVIRONMENT = null;

  private Cluster cluster;
  private Bucket bucket;
  private Collection collection;
  private String bucketName;

  private boolean upsert;
  private PersistTo persistTo;
  private ReplicateTo replicateTo;
  private Duration kvTimeout;
  private boolean kv;
  private String host;
  private Duration documentExpiry; // roughly 60 seconds with the 1 second sleep, not 100% accurate.
  private boolean adhoc;
  private int maxParallelism;

  @Override
  public void init() throws DBException {
    Properties properties = getProperties();

    host = properties.getProperty(HOST, "127.0.0.1");
    bucketName = properties.getProperty(BUCKET, "default");

    upsert = properties.getProperty(UPSERT, "false").equals("true");
    persistTo = parsePersistTo(properties.getProperty(PERSIST_TO, "0"));
    replicateTo = parseReplicateTo(properties.getProperty(REPLICATE_TO, "0"));
    kv = properties.getProperty(KV, "true").equals("true");
    documentExpiry = Duration.of(parseInt(properties.getProperty(DOCUMENT_EXPIRY, "0")), MILLIS);
    adhoc = properties.getProperty("couchbase.adhoc", "false").equals("true");
    maxParallelism = Integer.parseInt(properties.getProperty("couchbase.maxParallelism", "1"));

    try {
      synchronized (LOCK) {
        if (ENVIRONMENT == null) {
          ENVIRONMENT = ClusterEnvironment.builder()
              .securityConfig(createSecurityConfig())
              .ioConfig(createIoConfig())
              .timeoutConfig(createTimeoutConfig())
              .transcoder(createTranscoder())
              .build();
        }
        // initialize the connection
        cluster = Cluster.connect(host, createClusterOptions());
        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.parse("PT300S"));
        kvTimeout = ENVIRONMENT.timeoutConfig().kvTimeout();

        collection = bucket.defaultCollection();
        if (!kv) {
          cluster.queryIndexes().createPrimaryIndex(bucketName,
              createPrimaryQueryIndexOptions().ignoreIfExists(true));
        }
        logParams();
      }
    } catch (Exception exception) {
      throw new DBException("Can't connect to Couchbase", exception);
    }
  }

  private ClusterOptions createClusterOptions() {
    Properties properties = getProperties();
    String username = properties.getProperty(USERNAME, "");
    String password = properties.getProperty(PASSWORD, "");
    return clusterOptions(username, password).environment(ENVIRONMENT);
  }

  private SecurityConfig.Builder createSecurityConfig() {
    return SecurityConfig
        .enableTls(true)
        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
  }

  private IoConfig.Builder createIoConfig() {
    return IoConfig
        .enableDnsSrv(true);
  }

  private TimeoutConfig.Builder createTimeoutConfig() {
    Duration timeout = Duration.parse("PT60S");
    return TimeoutConfig
        .connectTimeout(timeout)
        .kvTimeout(timeout);
  }

  private JsonTranscoder createTranscoder() {
    return JsonTranscoder.create(SERIALIZER);
  }

  private void logParams() {
    StringBuilder params = new StringBuilder();

    params.append("host=").append(host);
    params.append(", bucket=").append(bucketName);
    params.append(", upsert=").append(upsert);
    params.append(", persistTo=").append(persistTo);
    params.append(", replicateTo=").append(replicateTo);
    params.append(", kv=").append(kv);

    LOGGER.info("===> Using params: " + params.toString());
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    String docId = getId(table, key);
    try {
      return kv ? readKv(docId, fields, result) : readN1ql(docId, fields, result);
    } catch (Exception exception) {
      LOGGER.error("Read failed docId {}", docId);
      return Status.ERROR;
    }
  }

  private Status readKv(String docId, Set<String> fields,
                        Map<String, ByteIterator> result) {
    GetOptions options = getOptions()
        .timeout(kvTimeout);
    if (fields != null && fields.size() <= 16) {
      options.project(fields);
    }
    result.putAll(collection.get(docId, options).contentAs(RESULT_TYPE));
    return Status.OK;
  }

  private Status readN1ql(String docId, Set<String> fields,
                          Map<String, ByteIterator> result) {
    throw new UnsupportedOperationException("N1QL is not supported");
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    if (upsert) {
      return upsert(table, key, values);
    }
    String docId = getId(table, key);
    try {
      return kv ? updateKv(docId, values) : updateN1ql(docId, values);
    } catch (Exception exception) {
      LOGGER.error("Update failed docId {}", docId);
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

  private Status updateKv(String docId,
                          Map<String, ByteIterator> values) {
    collection.replace(docId, values, replaceOptions()
        .timeout(kvTimeout)
        .expiry(documentExpiry)
        .durability(persistTo, replicateTo));
    return Status.OK;
  }

  private Status updateN1ql(String docId,
                            Map<String, ByteIterator> values) {
    throw new UnsupportedOperationException("N1QL is not supported");
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    if (upsert) {
      return upsert(table, key, values);
    }
    String docId = getId(table, key);
    try {
      if (kv) {
        return insertKv(docId, values);
      } else {
        return insertN1ql(docId, values);
      }
    } catch (Exception exception) {
      LOGGER.error("Insert failed docId {}", docId);
      return Status.ERROR;
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

  private Status insertKv(String docId,
                          Map<String, ByteIterator> values) {
    for (int i = 0; i < TRIES; i++) {
      try {
        collection.insert(docId, values, insertOptions()
            .timeout(kvTimeout)
            .expiry(documentExpiry)
            .durability(persistTo, replicateTo)
        );
        return Status.OK;
      } catch (TemporaryFailureException exception) {
        backoff(exception);
      }
    }
    throw new RuntimeException(format("Receiving TMPFAIL from the server after %d times", TRIES));
  }

  private void backoff(Exception exception) {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          format("Interrupted while backoff upon %s", exception.getMessage()));
    }
  }

  private Status insertN1ql(String docId,
                            Map<String, ByteIterator> values) {
    throw new UnsupportedOperationException("N1QL are not implemented");
  }

  private Status upsert(String table,
                        String key,
                        Map<String, ByteIterator> values) {
    String docId = getId(table, key);
    try {
      if (kv) {
        return upsertKv(docId, values);
      } else {
        return upsertN1ql(docId, values);
      }
    } catch (Exception exception) {
      LOGGER.error("Upsert failed docId {}", docId);
      return Status.ERROR;
    }
  }

  private Status upsertKv(String docId,
                          Map<String, ByteIterator> values) {
    collection.upsert(docId, values, upsertOptions()
        .timeout(kvTimeout)
        .expiry(documentExpiry)
        .durability(persistTo, replicateTo)
    );
    return Status.OK;
  }

  private Status upsertN1ql(String docId,
                            Map<String, ByteIterator> values) {
    throw new UnsupportedOperationException("N1QL queries not supported");
  }

  @Override
  public Status delete(String table, String key) {
    String docId = getId(table, key);
    try {
      if (kv) {
        return deleteKv(docId);
      } else {
        return deleteN1ql(docId);
      }
    } catch (Exception exception) {
      LOGGER.error("Delete failed docId {}", docId);
      return Status.ERROR;
    }
  }

  private Status deleteKv(String docId) {
    collection.remove(docId, removeOptions()
        .timeout(kvTimeout)
        .durability(persistTo, replicateTo));
    return Status.OK;
  }

  private Status deleteN1ql(String docId) {
    throw new UnsupportedOperationException("N1QL is not supported");
  }

  @Override
  public Status scan(String table, String startKey, int docCount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String docId = getId(table, startKey);
    try {
      String scanQuery = "SELECT " + fields(fields) + " FROM `" + bucketName
          + "` WHERE meta().id >= '$1' LIMIT $2";
      QueryResult scanResult = cluster.query(scanQuery, QueryOptions.queryOptions()
          .serializer(SERIALIZER)
          .parameters(JsonArray.from(docId, docCount))
          .adhoc(adhoc)
          .maxParallelism(maxParallelism)
      );
      result.addAll(getResult(scanResult));
      return Status.OK;
    } catch (Exception exception) {
      LOGGER.error("Scan failed start docId {} docCount {}", docId, docCount, exception);
      return Status.ERROR;
    }
  }

  @Override
  public Status query1(String table,
                       String filterField, String filterValue,
                       int offset, int recordCount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("query1 is not implemented");
  }

  @Override
  public Status query2(String table,
                       String filterField1, String filterValue1,
                       String filterField2, String filterValue2,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("query2 is not implemented");
  }

  @Override
  public Status query3(String table,
                       String filterField1, String filterValue1,
                       String filterField2, String filterValue2,
                       Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("query3 is not implemented");
  }

  private List<HashMap<String, ByteIterator>> getResult(QueryResult scanResult) {
    return scanResult.rowsAsObject().stream()
        .map(object -> object.getObject(bucketName))
        .filter(Objects::nonNull)
        .map(object -> object.toMap().entrySet().stream()
            .collect(toMap(
                Map.Entry::getKey,
                entry -> (ByteIterator) new StringByteIterator((String) entry.getValue()),
                (value1, value2) -> value1,
                HashMap::new))).collect(toList());
  }

  private static String fields(Set<String> fields) {
    if (fields == null || fields.isEmpty()) {
      return "*";
    }
    StringBuilder builder = new StringBuilder();
    for (Iterator<String> iterator = fields.iterator(); iterator.hasNext(); ) {
      String field = iterator.next();
      builder.append("`").append(field).append("`");
      if (iterator.hasNext()) {
        builder.append(",");
      }
    }
    return builder.toString();
  }
}