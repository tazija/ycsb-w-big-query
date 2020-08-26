package com.yahoo.ycsb.db.couchbase3;

import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_OBJECT;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.lang.Integer.parseInt;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.stream;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.BitSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Loads json documents from specified files from source directory
 */
public class Couchbase3Loader implements AutoCloseable {
  private static final String THREADCOUNT = "loader.threadcount";
  private static final String DIRECTORY = "loader.directory";
  private static final String FILES = "loader.files";
  private static final String ID_FIELD = "loader.idField";

  private static final String DEFAULT_ID_FIELD = "_id";
  private static final String DEFAULT_THREADCOUNT = "0";

  private final Logger logger = getLogger(getClass());
  private final ObjectMapper mapper = new ObjectMapper();
  private final Couchbase3Operations operations;
  private final String directory;
  private final List<String> files;
  private final String idField;
  private final ExecutorService executor;

  public Couchbase3Loader(String config) throws Exception {
    Properties properties = new Properties();
    properties.load(new FileInputStream(config));
    operations = new Couchbase3Operations(properties);

    int threadCount = parseInt(properties.getProperty(THREADCOUNT, DEFAULT_THREADCOUNT));
    executor = threadCount == 0 ? newDirectExecutorService() : newFixedThreadPool(threadCount);
    directory = checkNotNull(properties.getProperty(DIRECTORY));
    files = stream(properties.getProperty(FILES).split(","))
        .map(String::trim).filter(file -> !file.isEmpty()).collect(toList());
    idField = properties.getProperty(ID_FIELD, DEFAULT_ID_FIELD);
  }

  public void load() throws Exception {
    State state = new State(files);
    CompletableFuture.allOf(
        IntStream.range(0, files.size())
            .mapToObj(fileIndex -> runAsync(() -> {
                  load(fileIndex, state);
                }, executor)
            ).toArray(CompletableFuture[]::new)
    ).get();
  }

  private void load(int fileIndex, State state) {
    String file = state.getFile(fileIndex);
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
      state.setFileLoaded(fileIndex, true);
      logger.info("Loaded file {} progress {}%", file, state.getProgress());
    } catch (Exception exception) {
      state.setFileLoaded(fileIndex, false);
      logger.error("Failure loading file {} progress {}%", file, state.getProgress(), exception);
    }
  }

  @Override
  public void close() {
    executor.shutdown(); // disable new tasks from being submitted
    try {
      // wait a while for existing tasks to terminate
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        // cancel currently executing tasks
        executor.shutdownNow();
      }
    } catch (InterruptedException exception) {
      // cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      currentThread().interrupt();
    }
  }

  class State {
    private final List<String> files;
    private final BitSet loadedFiles;
    private final BitSet failedFiles;

    public State(List<String> files) {
      this.files = files;
      this.loadedFiles = new BitSet(files.size());
      this.failedFiles = new BitSet(files.size());
    }

    public String getFile(int fileIndex) {
      return files.get(fileIndex);
    }

    public int getProgress() {
      return (int) (((failedFiles.cardinality() +
          loadedFiles.cardinality()) / (float) files.size()) * 100);
    }

    public void setFileLoaded(int fileIndex, boolean loaded) {
      synchronized (this) {
        (loaded ? loadedFiles : failedFiles).set(fileIndex, true);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    try (Couchbase3Loader loader = new Couchbase3Loader(args[0])) {
      loader.load();
    }
  }
}