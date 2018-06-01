/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.model;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.NumericByteIterator;
import com.yahoo.ycsb.RandomByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.NumberGenerator;

/**
 * Type for model.
 */
public enum Type {
  STRING,
  DOUBLE;

  public ByteIterator getByteIterator(String key, String fieldkey, NumberGenerator fieldlengthgenerator) {
    switch (this) {
    case DOUBLE:
      return new NumericByteIterator(fieldlengthgenerator.nextValue().doubleValue());
    case STRING:
      return new StringByteIterator(buildDeterministicValue(key, fieldkey, fieldlengthgenerator));
    default:
      return new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
    }
  }

  /**
   * Build a deterministic value given the key information.
   */
  private String buildDeterministicValue(String key, String fieldkey, NumberGenerator fieldlengthgenerator) {
    int size = fieldlengthgenerator.nextValue().intValue();
    StringBuilder sb = new StringBuilder(size);
    sb.append(key);
    sb.append(':');
    sb.append(fieldkey);
    while (sb.length() < size) {
      sb.append(':');
      sb.append(sb.toString().hashCode());
    }
    sb.setLength(size);

    return sb.toString();
  }

}
