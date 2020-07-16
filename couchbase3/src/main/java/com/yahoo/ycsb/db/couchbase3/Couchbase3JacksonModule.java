package com.yahoo.ycsb.db.couchbase3;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import java.io.IOException;

public class Couchbase3JacksonModule extends SimpleModule {
  public Couchbase3JacksonModule() {
    // register custom serializer and deserializer for ycsb byte iterator
    addDeserializer(ByteIterator.class, new JsonDeserializer<ByteIterator>() {
      @Override
      public ByteIterator deserialize(JsonParser parser,
                                      DeserializationContext context) throws IOException {
        return new StringByteIterator(new String(parser.getBinaryValue()));
      }
    });
    addSerializer(ByteIterator.class, new JsonSerializer<ByteIterator>() {
      @Override
      public void serialize(ByteIterator bytes,
                            JsonGenerator generator,
                            SerializerProvider provider) throws IOException {
        generator.writeString(bytes.toString());
      }
    });
  }
}