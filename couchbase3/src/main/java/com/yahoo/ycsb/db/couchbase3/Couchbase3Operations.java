package com.yahoo.ycsb.db.couchbase3;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Utils.parsePersistTo;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Utils.parseReplicateTo;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.time.temporal.ChronoUnit.MILLIS;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.codec.JacksonJsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonValueModule;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import com.couchbase.client.java.query.QueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

public class Couchbase3Operations implements Couchbase3Config {

  private static final JacksonJsonSerializer SERIALIZER = JacksonJsonSerializer.create(
      new ObjectMapper()
          .registerModule(new JsonValueModule())
          .registerModule(new Couchbase3JacksonModule())
  );

  private final Cluster cluster;
  private final Bucket bucket;
  private final Collection collection;
  private final String bucketName;
  private final boolean upsert;
  private final PersistTo persistTo;
  private final ReplicateTo replicateTo;
  private final Duration kvTimeout;
  private final boolean kv;
  private final String host;
  // roughly 60 seconds with the 1 second sleep, not 100% accurate
  private final Duration documentExpiry;
  private final boolean adhoc;
  private final int maxParallelism;

  public Couchbase3Operations(Properties properties) {
    host = properties.getProperty(HOST, DEFAULT_HOST);
    bucketName = properties.getProperty(BUCKET, DEFAULT_BUCKET);

    upsert = parseBoolean(properties.getProperty(UPSERT, DEFAULT_UPSERT));
    persistTo = parsePersistTo(properties.getProperty(PERSIST_TO, DEFAULT_PERSIST_TO));
    replicateTo = parseReplicateTo(properties.getProperty(REPLICATE_TO, DEFAULT_REPLICATE_TOO));
    kv = parseBoolean(properties.getProperty(KV, DEFAULT_KV));
    documentExpiry = Duration.of(parseInt(properties.getProperty(DOCUMENT_EXPIRY, DEFAULT_DOCUMENT_EXPIRY)), MILLIS);
    adhoc = parseBoolean(properties.getProperty(ADHOC, DEFAULT_ADHOC));
    maxParallelism = parseInt(properties.getProperty(MAX_PARALLELISM, DEFAULT_MAX_PARALLELISM));

    ClusterEnvironment environment = ClusterEnvironment.builder()
        .securityConfig(createSecurityConfig())
        .ioConfig(createIoConfig())
        .timeoutConfig(createTimeoutConfig())
        .transcoder(JsonTranscoder.create(SERIALIZER))
        .build();

    String username = properties.getProperty(USERNAME, "");
    String password = properties.getProperty(PASSWORD, "");
    ClusterOptions options = clusterOptions(username, password).environment(environment);

    // initialize the connection
    cluster = Cluster.connect(host, options);
    bucket = cluster.bucket(bucketName);

    String waitUntilReady = properties.getProperty(WAIT_UNTIL_READY, DEFAULT_WAIT_UNTIL_READY);
    bucket.waitUntilReady(Duration.parse(waitUntilReady));

    kvTimeout = environment.timeoutConfig().kvTimeout();
    collection = bucket.defaultCollection();
  }

  protected SecurityConfig.Builder createSecurityConfig() {
    return SecurityConfig
        .enableTls(true)
        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
  }

  protected IoConfig.Builder createIoConfig() {
    return IoConfig
        .enableDnsSrv(true);
  }

  protected TimeoutConfig.Builder createTimeoutConfig() {
    Duration timeout = Duration.parse(DEFAULT_TIMEOUT);
    return TimeoutConfig
        .connectTimeout(timeout)
        .kvTimeout(timeout);
  }

  public GetResult get(String id, Set<String> fields) {
    GetOptions options = getOptions()
        .timeout(kvTimeout);
    if (fields != null) {
      options.project(fields);
    }
    if (kv) {
      return collection.get(id, options);
    } else {
      throw new UnsupportedOperationException("n1ql is not implemented");
    }
  }

  public QueryResult query(String query,
                           JsonArray parameters) {
    return cluster.query(query, queryOptions()
        .serializer(SERIALIZER)
        .parameters(parameters)
        .adhoc(adhoc)
        .maxParallelism(maxParallelism)
    );
  }

  public void insert(String id, Object content) {
    if (upsert) {
      upsert(id, content);
    } else if (kv) {
      collection.insert(id, content, insertOptions()
          .timeout(kvTimeout)
          .expiry(documentExpiry)
          .durability(persistTo, replicateTo)
      );
    } else {
      throw new UnsupportedOperationException("n1ql is not implemented");
    }
  }

  public void update(String id, Object content) {
    if (upsert) {
      upsert(id, content);
    } else if (kv) {
      collection.replace(id, content, replaceOptions()
          .timeout(kvTimeout)
          .expiry(documentExpiry)
          .durability(persistTo, replicateTo));
    } else {
      throw new UnsupportedOperationException("n1ql is not implemented");
    }
  }

  public void upsert(String id, Object content) {
    if (kv) {
      collection.upsert(id, content, upsertOptions()
          .timeout(kvTimeout)
          .expiry(documentExpiry)
          .durability(persistTo, replicateTo)
      );
    } else {
      throw new UnsupportedOperationException("n1ql is not implemented");
    }
  }

  public void remove(String id) {
    if (kv) {
      collection.remove(id, removeOptions()
          .timeout(kvTimeout)
          .durability(persistTo, replicateTo));
    } else {
      throw new UnsupportedOperationException("n1ql is not implemented");
    }
  }

  public Cluster getCluster() {
    return cluster;
  }

  public Bucket getBucket() {
    return bucket;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getHost() {
    return host;
  }

  public boolean isKv() {
    return kv;
  }
}