package org.emma.spark.streaming.integration.kafka;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.emma.spark.streaming.testcontainer.kafka.KafkaContainerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.emma.spark.streaming.constants.Constants.CONFLUENT_PLATFORM_VERSION;

/**
 * Add test cases for {@link org.emma.spark.streaming.testcontainer.kafka.KafkaContainerCluster},
 * which copied from testcontainer repository's
 * examples/kafka-cluster/src/test/java/com/example/kafkacluster/KafkaContainerClusterTest.java
 */
public class KafkaContainerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerIntegrationTest.class);

    @Test
    public void testKafkaContainerCluster() throws Exception {
        int BROKER_NUM = 3;
        int TOPIC_REPLICATION_CNT = 2;

        try (KafkaContainerCluster cluster =
                     new KafkaContainerCluster(CONFLUENT_PLATFORM_VERSION, BROKER_NUM, TOPIC_REPLICATION_CNT)) {
            cluster.start();

            Unreliables.retryUntilTrue(100 * 1000,
                    TimeUnit.MICROSECONDS,
                    () -> {
                        return cluster.isAllBrokerRunning();
                    });
            String bootstrapServers = cluster.getBootstrapServers();

            Assertions.assertTrue(cluster.isAllBrokerRunning());
            Assertions.assertTrue(StringUtils.isNotBlank(bootstrapServers));
            Assertions.assertEquals(cluster.getBrokers().size(), BROKER_NUM);

            for (KafkaContainer kafkaContainer : cluster.getBrokers()) {
                Assertions.assertTrue(kafkaContainer.getMappedPort(9093) > 0);
            }

            testKafkaFunctionality(bootstrapServers, BROKER_NUM, TOPIC_REPLICATION_CNT);
        }
    }


    private void testKafkaFunctionality(String bootstrapServers, int partitions, int topicReplicationCnt) throws Exception {
        try (
                // create kafka admin client
                AdminClient adminClient = AdminClient.create(
                        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));

                // create kafka producer
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        ImmutableMap.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()),
                        new StringSerializer(),
                        new StringSerializer());

                // create kafka consumer
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        ImmutableMap.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                String.format("tc-%s", UUID.randomUUID().toString()),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"
                        ),
                        new StringDeserializer(),
                        new StringDeserializer());
        ) {
            String topicName = "KafkaTopicName-" + UUID.randomUUID().toString();
            // create new topic instance
            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, partitions, (short) topicReplicationCnt));

            // send create new topic to kafka server side and set timeout as 30 seconds
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            // let consumer side subscribe from the new created topic
            consumer.subscribe(Collections.singletonList(topicName));

            // let producer side generate records and publish to new created topic
            producer.send(new ProducerRecord<>(topicName, "Key-" + UUID.randomUUID().toString(), "Value-" + UUID.randomUUID().toString()));

            // here let all the init instances to execute
            Unreliables.retryUntilTrue(10,
                    TimeUnit.SECONDS, () -> {
                        // poll message from the kakfa brokers
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        if (records.isEmpty()) {
                            // not poll message this time, continue the loop
                            return false;
                        }

                        Assertions.assertTrue(!records.isEmpty());

                        Set<TopicPartition> topicPartitions = records.partitions();
                        Assertions.assertTrue(CollectionUtils.isNotEmpty(topicPartitions));

                        for (TopicPartition topicPartition : topicPartitions) {
                            int partitionId = topicPartition.partition();
                            String content = topicPartition.toString();
                            LOG.info("#testKafkaFunctionality partitionId {}, content {}", partitionId, content);
                        }

                        return true;
                    }
            );
            LOG.info("#testKafkaFunctionality finish test cases let consumer un-subscribe from the brokers");
            consumer.unsubscribe();
        }
    }
}
