package org.etl.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

import org.etl.ConfigUtil;


public class StreamConsumer {
  private final StreamsConfig config;
  private ExecutorService executor;

  public StreamConsumer() {
    Properties props = createConsumerConfig();
    config = new StreamsConfig(props);
  }

  public void execute(StreamsBuilder[] builderList, int numberOfThreads) {
    executor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

    for(StreamsBuilder builder: builderList) {
      executor.submit(new StreamHandler(builder, config));
    }
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

  public void shutdown() {
    if (executor != null) {
      executor.shutdown();
    }
    try {
      if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
        System.out
            .println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
      }
    } catch (InterruptedException e) {
      System.out.println("Interrupted during shutdown, exiting uncleanly");
    }
  }
}
