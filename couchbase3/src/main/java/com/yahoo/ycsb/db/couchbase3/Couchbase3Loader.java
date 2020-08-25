package com.yahoo.ycsb.db.couchbase3;

import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

/**
 * Loads json documents from specified files from source directory
 */
public class Couchbase3Loader {

  private static final String DIRECTORY = "loader.directory";
  private static final String FILES = "loader.files";
  private static final String ID_FIELD = "loader.idField";
  private static final String DEFAULT_ID_FIELD = "_id";

  private final ObjectMapper mapper = new ObjectMapper();
  private final Couchbase3Operations operations;
  private final String directory;
  private final List<String> files;
  private final String idField;

  public Couchbase3Loader(String config) throws Exception {
    Properties properties = new Properties();
    properties.load(new FileInputStream(config));
    operations = new Couchbase3Operations(properties);

    directory = checkNotNull(properties.getProperty(DIRECTORY));
    files = stream(properties.getProperty(FILES).split(","))
        .map(String::trim).filter(file -> !file.isEmpty()).collect(toList());
    idField = properties.getProperty(ID_FIELD, DEFAULT_ID_FIELD);
  }

  public void load() throws Exception {
    for (String file : files) {
      load(file);
    }
  }

  private void load(String file) throws Exception {
    try (JsonParser parser = mapper.getFactory().createParser(new File(directory, file))) {
      if (parser.nextToken() != START_ARRAY) {
        throw new IllegalStateException("Array is expected");
      }
      while (parser.nextToken() == START_OBJECT) {
        // read everything from this START_OBJECT to the matching END_OBJECT
        // and return it as a tree model ObjectNode
        ObjectNode node = mapper.readTree(parser);
        String id = node.remove(idField).asText();
        operations.upsert(id, node);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Couchbase3Loader loader = new Couchbase3Loader(args[0]);
    loader.load();
  }
}
