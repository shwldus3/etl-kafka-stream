package org.etl.topics;

import org.apache.kafka.common.serialization.Serde;

public interface TopicService<T> {
  Serde<String> keySerde();
  Serde<T> valueSerde();
  String name();
}
