package org.etl.kafka;

import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.etl.model.DBService;
import org.etl.topics.TopicService;
import org.etl.schema.FormatConverter;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeduplicationStream<T> {
  private final static StreamsBuilder builder = new StreamsBuilder();
  private Logger logger = LoggerFactory.getLogger(this.getClass());
  private FormatConverter fc = new FormatConverter<>();
  private TopicService<T> topicInfo;
  private DBService dbService;

  public DeduplicationStream(TopicService<T> topicInfo, DBService dbService) {
    this.topicInfo = topicInfo;
    this.dbService = dbService;
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
          dbService.process(fc.getQuery(windowedKey.key()));
        });
  }

  public StreamsBuilder getBuilder() {
    return builder;
  }
}
