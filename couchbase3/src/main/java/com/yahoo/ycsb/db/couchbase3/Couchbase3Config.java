package com.yahoo.ycsb.db.couchbase3;

public interface Couchbase3Config {
  String HOST = "couchbase.host";
  String BUCKET = "couchbase.bucket";
  String USERNAME = "couchbase.username";
  String PASSWORD = "couchbase.password";
  String PERSIST_TO = "couchbase.persistTo";
  String REPLICATE_TO = "couchbase.replicateTo";
  String UPSERT = "couchbase.upsert";
  String KV = "couchbase.kv";
  String DOCUMENT_EXPIRY = "couchbase.documentExpiry";
  String ADHOC = "couchbase.adhoc";
  String MAX_PARALLELISM = "couchbase.maxParallelism";
  String WAIT_UNTIL_READY = "couchbase.waitUntilReady";

  String DEFAULT_HOST = "127.0.0.1";
  String DEFAULT_BUCKET = "default";
  String DEFAULT_UPSERT = "false";
  String DEFAULT_PERSIST_TO = "0";
  String DEFAULT_REPLICATE_TOO = "0";
  String DEFAULT_KV = "true";
  String DEFAULT_DOCUMENT_EXPIRY = "0";
  String DEFAULT_ADHOC = "false";
  String DEFAULT_MAX_PARALLELISM = null;
  String DEFAULT_WAIT_UNTIL_READY = "PT300S";
  String DEFAULT_TIMEOUT = "PT60S";
}