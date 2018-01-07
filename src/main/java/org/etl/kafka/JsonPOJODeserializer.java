package org.etl.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonPOJODeserializer<T> implements Deserializer<T> {
  private ObjectMapper objectMapper = new ObjectMapper();
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  private Class<T> tClass;

  /**
  * Default constructor needed by Kafka
  */
  public JsonPOJODeserializer(Class<T> tClass) {
    this.tClass = tClass;
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
  }

  @Override
  public T deserialize(String topic, byte[] bytes) {
    if (bytes == null)
      return null;

    T data;
    try {
      data = objectMapper.readValue(bytes, tClass);
    } catch (Exception e) {
      logger.error("JSON 형태의 메세지가 아닙니다. 다시 시도해주세요");
      return null;
    }

    return data;
  }

  @Override
  public void close() {
  }
}
