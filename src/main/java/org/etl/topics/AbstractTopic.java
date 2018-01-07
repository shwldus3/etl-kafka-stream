package org.etl.topics;

import org.apache.kafka.common.serialization.Serde;

public class AbstractTopic<T> implements TopicService {
  private String name;
  private Serde<String> keySerde;
  private Serde<T> valueSerde;

  public Serde<String> keySerde() { return keySerde; }
  public Serde<T> valueSerde() { return valueSerde; }
  public String name() { return name; }
}
