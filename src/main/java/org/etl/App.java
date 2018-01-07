package org.etl;

import org.apache.kafka.streams.StreamsBuilder;
import org.etl.kafka.DeduplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.etl.kafka.StreamConsumer;

public class App {
  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(App.class);
    logger.info("Start ETL App");

    String className = "Event";
    String task1TopicName = new ConfigUtil().getProperty("kafka.task1.topic");
    String task2TopicName = new ConfigUtil().getProperty("kafka.task2.topic");


    StreamsBuilder task1 = new DeduplicationStream<>(task1TopicName, className).getBuilder();
    StreamsBuilder task2 = new DeduplicationStream<>(task2TopicName, className).getBuilder();
    StreamsBuilder[] taskList = {task1, task2};

    StreamConsumer streamConsumer = new StreamConsumer();
    streamConsumer.execute(taskList, 2);

    logger.info("Start Kafka Stream");

    streamConsumer.shutdown();
  }
}
