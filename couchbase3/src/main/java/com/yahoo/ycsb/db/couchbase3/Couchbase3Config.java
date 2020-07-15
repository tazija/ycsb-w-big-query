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
}