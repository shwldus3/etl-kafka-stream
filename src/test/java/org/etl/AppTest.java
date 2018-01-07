package org.etl;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.etl.kafka.DeduplicationStream;
import org.etl.kafka.StreamConsumer;

import org.junit.Assert;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.concurrent.Future;

import org.etl.model.DBDelegate;
import org.etl.topics.TopicDelegate;
import org.etl.topics.TopicService;
import org.etl.model.DBService;
import java.util.Properties;


public class AppTest extends TestCase {
  @ClassRule
  public static DeduplicationStream CLUSTER;
  private static ConfigUtil cfg = new ConfigUtil();
  private static TopicService topicInfo;

  @BeforeClass
  public static void startKafkaCluster() {
    String className = "Event";
    String topicName = cfg.getProperty("kafka.task.topic");

    topicInfo = new TopicDelegate(topicName, className, "JSON").getTopicInfo();
    DBService dbService = new DBDelegate("MySQL").getTargetService();
    CLUSTER = new DeduplicationStream<>(topicInfo, dbService);

  }

  @Test
  public void testProcess() throws Exception {
    final List<String> inputValues = Arrays.asList(
        "{\"event_id\": 1, \"event_timestamp\": 19981231235959}",
        "{\"event_id\": 2, \"event_timestamp\": 19981231235959}",
        "{\"event_id\": 2, \"event_timestamp\": 19981231235959}",
        "{\"event_id\": 1, \"event_timestamp\": 19981231235959, \"service_code\": \"TEST1\", \"event_context\": \"\"}",
        "{\"event_id\": 1, \"event_timestamp\": 19981231235959, \"service_code\": \"TEST1\", \"event_context\": \"\"}",
        "{\"event_id\": 2, \"event_timestamp\": 19981231235959, \"service_code\": \"TEST2\", \"event_context\": \"\"}"
    );

    final Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.brokerList"));
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    System.out.println(topicInfo.name());
    produceValuesSynchronously(topicInfo.name(), inputValues, producerConfig);

    StreamsBuilder task = CLUSTER.getBuilder();
    StreamConsumer streamConsumer = new StreamConsumer(task);

    //TODO: Wait stream result...
    Assert.assertTrue(true);
  }

  private static <V> void produceValuesSynchronously(String topic, Collection<V> records, Properties producerConfig) throws InterruptedException, ExecutionException{
    Collection<KeyValue<Object, V>> keyedRecords = records.stream().map(record -> new KeyValue<>(null, record)).collect(Collectors.toList());
    produceKeyValuesSynchronously(topic, keyedRecords, producerConfig);
  }

  private static <K, V> void produceKeyValuesSynchronously(String topic, Collection<KeyValue<K, V>> records, Properties producerConfig) throws InterruptedException, ExecutionException {
    Producer<K, V> producer = new KafkaProducer<>(producerConfig);

    for (KeyValue<K, V> record : records) {
      Future<RecordMetadata> f = producer.send(new ProducerRecord<>(topic, record.key, record.value));
      f.get();
    }
    producer.flush();
    producer.close();
  }
}
