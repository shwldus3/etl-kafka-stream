package org.etl.kafka;

import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.etl.model.DBConnection;
import java.util.concurrent.TimeUnit;
import org.etl.kafka.TopicService.Topic;

public class DeduplicationStream<T> extends StreamUtil<T> {
  private final static StreamsBuilder builder = new StreamsBuilder();
  private final static DBConnection db = new DBConnection();
  private Logger logger = LoggerFactory.getLogger(this.getClass());
  private Topic<String, T> topicInfo;

  public DeduplicationStream(String topicName, String className) {
    String schemaPackage = "org.etl.schema";

    try {
      Class topicClass = Class.forName(schemaPackage + "." + className);
      topicInfo = new TopicService.JsonTopic(topicName, topicClass).getTopicInfo();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    createStream();
  }

  private void createStream() {
    KStream<String, T> stream = builder.stream(topicInfo.name(), Consumed.with(topicInfo.keySerde(), topicInfo.valueSerde()));

    long windowSizeMs = TimeUnit.MINUTES.toMillis(1);

    stream
        .groupBy((key, value) -> value, Serialized.with(topicInfo.valueSerde(), topicInfo.valueSerde()))
        .windowedBy(TimeWindows.of(windowSizeMs))
        .count()
        .toStream()
        .foreach((Windowed<T> windowedKey, Long count) -> {
          logger.info(windowedKey.key().toString());
          String query = super.getQuery(windowedKey.key());
          db.executeQuery(query);
        });
  }

  public StreamsBuilder getBuilder() {
    return builder;
  }
}
