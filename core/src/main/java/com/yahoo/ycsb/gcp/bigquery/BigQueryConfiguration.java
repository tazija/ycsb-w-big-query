/**
 * Copyright (c) 2018 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
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
package com.yahoo.ycsb.gcp.bigquery;

import com.yahoo.ycsb.model.Type;

import java.util.Map;
import java.util.Objects;

/**
 * All necessary configuration for connection to BigQuery.
 */
public class BigQueryConfiguration {

  public static final String BIG_QUERY_PROJECT_ID = "bigquery.projectid";
  public static final String BIG_QUERY_DATA_SET_NAME = "bigquery.dataSetName";
  public static final String BIG_QUERY_TABLE_NAME = "bigquery.tableName";
  public static final String BIG_QUERY_PATH_TO_KEY = "bigquery.pathToKey";
  public static final String BIG_QUERY_MODEL = "bigquery.model";


  private String projectId;
  private String dataSetName;
  private String tableName;
  private String pathToKey;
  private Map<String, Type> model;

  public BigQueryConfiguration(String projectId, String dataSetName, String tableName, String pathToKey,
                               Map<String, Type> model) {
    this.projectId = projectId;
    this.dataSetName = dataSetName;
    this.tableName = tableName;
    this.pathToKey = pathToKey;
    this.model = model;
  }

  public boolean isPropertyCorrect() {
    return Objects.nonNull(projectId) &&
        Objects.nonNull(tableName) &&
        Objects.nonNull(dataSetName) &&
        Objects.nonNull(pathToKey) &&
        Objects.nonNull(model);
  }
  public String getProjectId() {
    return projectId;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPathToKey() {
    return pathToKey;
  }

  public Map<String, Type> getModel() {
    return model;
  }
}
