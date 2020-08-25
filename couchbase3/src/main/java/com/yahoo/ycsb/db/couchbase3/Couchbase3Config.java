package com.yahoo.ycsb.db.couchbase3;

/**
 * <p> The following options can be passed when using this database client to override the defaults
 *
 * <ul>
 * <li><b>couchbase.host=127.0.0.1</b> The hostname from one server</li>
 * <li><b>couchbase.bucket=default</b> The bucket name to use</li>
 * <li><b>couchbase.password=</b> The password of the bucket</li>
 * <li><b>couchbase.syncMutationResponse=true</b> If mutations should wait for the response to complete</li>
 * <li><b>couchbase.persistTo=0</b> Persistence durability requirement</li>
 * <li><b>couchbase.replicateTo=0</b> Replication durability requirement</li>
 * <li><b>couchbase.upsert=false</b> Use upsert instead of insert or replace</li>
 * <li><b>couchbase.kv=true</b> If set to false, mutation operations will also be performed through N1QL</li>
 * <li><b>couchbase.documentExpiry=0</b> Document expiry is the amount of time until a document expires</li>
 * </ul>
 */
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
  String DEFAULT_MAX_PARALLELISM = "1";
  String DEFAULT_WAIT_UNTIL_READY = "PT300S";
  String DEFAULT_TIMEOUT = "PT60S";
}