package org.etl.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class TopicService {
  public static class Topic<K, V> {
    private String name;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
      this.name = name;
      this.keySerde = keySerde;
      this.valueSerde = valueSerde;
    }

    public Serde<K> keySerde() {
            return keySerde;
        }

    public Serde<V> valueSerde() {
            return valueSerde;
        }

    public String name() {
            return name;
        }
  }

  public static class JsonTopic<T> {
    private final Topic<String, T> topicInfo;

    public JsonTopic(String topicName, Class<T> cls) {
      Map<String, Object> serdeProps = new HashMap<>();
      this.topicInfo = new Topic<>(topicName, Serdes.String(), createSerde(serdeProps, cls));
    }

    private Serde<T> createSerde(Map<String, Object> serdeProps, Class cls) {
      final Serializer<T> serializer = new JsonPOJOSerializer<>();
      serdeProps.put("JsonPOJOClass", cls);
      serializer.configure(serdeProps, false);

      final Deserializer<T> deserializer = new JsonPOJODeserializer<>();
      serdeProps.put("JsonPOJOClass", cls);
      deserializer.configure(serdeProps, false);

      return Serdes.serdeFrom(serializer, deserializer);
    }

    public Topic<String, T> getTopicInfo() {
      return topicInfo;
    }
  }
}
