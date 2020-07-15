package com.yahoo.ycsb.db.couchbase3;

import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.PERSIST_TO;
import static com.yahoo.ycsb.db.couchbase3.Couchbase3Config.REPLICATE_TO;
import static java.lang.String.format;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;

public interface Couchbase3Utils {

  static String getId(String table,
                      String key) {
    return table + ":" + key;
  }

  static PersistTo parsePersistTo(String property) {
    int value = Integer.parseInt(property);
    switch (value) {
      case 0:
        return PersistTo.NONE;
      case 1:
        return PersistTo.ONE;
      case 2:
        return PersistTo.TWO;
      case 3:
        return PersistTo.THREE;
      case 4:
        return PersistTo.FOUR;
      default:
        throw new IllegalArgumentException(format("%s must be between 0 and 4", PERSIST_TO));
    }
  }

  static ReplicateTo parseReplicateTo(final String property) {
    int value = Integer.parseInt(property);

    switch (value) {
      case 0:
        return ReplicateTo.NONE;
      case 1:
        return ReplicateTo.ONE;
      case 2:
        return ReplicateTo.TWO;
      case 3:
        return ReplicateTo.THREE;
      default:
        throw new IllegalArgumentException(format("%s must be between 0 and 4", REPLICATE_TO));
    }
  }
}
