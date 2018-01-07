package org.etl;

import org.apache.kafka.streams.StreamsBuilder;
import org.etl.kafka.DeduplicationStream;
import org.etl.model.DBDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.etl.kafka.StreamConsumer;
import org.etl.topics.TopicDelegate;
import org.etl.topics.TopicService;
import org.etl.model.DBService;

public class App {
  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(App.class);
    logger.info("Start ETL App");

    String className = "Event";
    String topicName = new ConfigUtil().getProperty("kafka.task.topic");

    TopicService topicInfo = new TopicDelegate(topicName, className, "JSON").getTopicInfo();
    DBService dbService = new DBDelegate("MySQL").getTargetService();

    DeduplicationStream deduplicationStream = new DeduplicationStream<>(topicInfo, dbService);

    StreamsBuilder task = deduplicationStream.getBuilder();
    new StreamConsumer(task);

    logger.info("Start Kafka Stream");
  }
}
