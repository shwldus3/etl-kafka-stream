package org.etl.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.etl.ConfigUtil;


public class StreamConsumer {
  private final StreamsConfig config;
  private KafkaStreams kafkaStreams;
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public StreamConsumer(StreamsBuilder builder) {
    Properties props = createConsumerConfig();
    config = new StreamsConfig(props);

    execute(builder);
  }

  private void execute(StreamsBuilder builder) {
    kafkaStreams = new KafkaStreams(builder.build(), config);

    kafkaStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
      logger.error("Unexpected Error in Kafka Stream");
      kafkaStreams.close();
    });

    kafkaStreams.start();
  }

  private static Properties createConsumerConfig() {
    Properties props = new Properties();
    ConfigUtil cfg = new ConfigUtil();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, cfg.getProperty("kafka.applicationId"));
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.brokerList"));
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

    return props;
  }
}
