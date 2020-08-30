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

import static java.lang.String.format;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.yahoo.ycsb.model.Type;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Api for connection to BiqQuery from GCP.
 */
public class BigQueryApi {

  private BigQuery bigQuery;
  private TableId tableId;

  public BigQueryApi(BigQueryConfiguration configuration) throws IOException {
    this(configuration.getProjectId(), configuration.getDataSetName(), configuration.getTableName(),
        configuration.getPathToKey(), configuration.getModel());
  }

  private BigQueryApi(String projectId, String dataSetName, String tableName, String pathToKey, Map<String, Type> model)
      throws IOException {

    this.bigQuery = BigQueryOptions.newBuilder().setProjectId(projectId)
        .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(pathToKey)))
        .build().getService();

    this.tableId = TableId.of(dataSetName, tableName);

    List<Field> fields = model
        .entrySet()
        .stream()
        .map(m -> Field.of(m.getKey(), m.getValue().getLegacySQLTypeName()))
        .collect(Collectors.toList());

    Schema schema = Schema.of(fields);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    if (this.bigQuery.getTable(tableId) == null) {
      this.bigQuery.create(tableInfo);
      System.out.printf("Creating new table %s", tableId);
    } else {
      System.out.printf("Using existing table %s", tableId);
    }
  }

  public void insert(List<Map<String, Object>> rows) {
    if (!rows.isEmpty()) {
      InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
      rows.forEach(builder::addRow);
      InsertAllRequest insertAllRequest = builder.build();

      final CompletableFuture<Void> future = CompletableFuture
          .supplyAsync(() -> bigQuery.insertAll(insertAllRequest))
          .thenAcceptAsync(insertAllResponse -> {
            if (insertAllResponse.hasErrors()) {
              bigQuery.insertAll(insertAllRequest);
            }
          });
      future.join();
    }
  }
}