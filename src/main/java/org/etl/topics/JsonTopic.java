package org.etl.topics;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.etl.kafka.JsonPOJODeserializer;
import org.etl.kafka.JsonPOJOSerializer;

public class JsonTopic<T> extends AbstractTopic {
  private static JsonTopic instance;
  private String name;
  private Serde<String> keySerde;
  private Serde<T> valueSerde;

  private JsonTopic(String topicName, Class<T> cls) {
    this.name = topicName;
    this.keySerde = Serdes.String();
    this.valueSerde = createSerde(cls);
  }

  public static JsonTopic createJsonTopic(String topicName, String className) throws ClassNotFoundException{
    if (instance == null) {
      String schemaPackage = "org.etl.schema";
      Class tClass = Class.forName(schemaPackage + "." + className);

      instance = new JsonTopic<>(topicName, tClass);
    }

    return instance;
  }

  private Serde<T> createSerde(Class<T> tClass) {
    Serializer<T> serializer = new JsonPOJOSerializer<>();
    Deserializer<T> deserializer = new JsonPOJODeserializer<>(tClass);

    return Serdes.serdeFrom(serializer, deserializer);
  }

  @Override
  public Serde<String> keySerde() {
    return keySerde;
  }

  @Override
  public Serde<T> valueSerde() {
    return valueSerde;
  }

  @Override
  public String name() {
    return name;
  }
}
