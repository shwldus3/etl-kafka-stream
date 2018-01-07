package org.etl.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamHandler implements Runnable{
  private KafkaStreams kafkaStreams;
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public StreamHandler(StreamsBuilder builder, StreamsConfig config) {
    kafkaStreams = new KafkaStreams(builder.build(), config);

    kafkaStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      logger.error("예상치 못한 에러가 발생했습니다.");
      kafkaStreams.close();
    });

    kafkaStreams.start();
  }

  public void run() {
    logger.info("Process: " + kafkaStreams.allMetadata() + ", By ThreadID: " + Thread.currentThread().getId());
  }
}
