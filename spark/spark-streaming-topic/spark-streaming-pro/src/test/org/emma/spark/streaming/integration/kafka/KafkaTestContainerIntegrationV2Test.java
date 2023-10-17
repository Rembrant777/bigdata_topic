package org.emma.spark.streaming.integration.kafka;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
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
import org.emma.spark.streaming.testcontainer.kafka.KafkaTestContainer;
import org.emma.spark.streaming.testcontainer.zk.ZookeeperTestContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Integrated Testing for {@link org.emma.spark.streaming.testcontainer.kafka.KafkaContainerCluster};
 * <p>
 * All test cases are moved to {@link KafkaContainerIntegrationTest}, this class will be deprecated.
 */
@Deprecated
public class KafkaTestContainerIntegrationV2Test {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestContainerIntegrationV2Test.class);

    @Test
    public void testKafkaContainerCluster() throws Exception {
        // params: confluentPlatformVersion, brokersNum, internalTopicRf
        try (KafkaContainerCluster cluster = new KafkaContainerCluster("6.2.1", 3, 2)) {
            cluster.start();
            String bootstrapServers = cluster.getBootstrapServers();
            Assertions.assertTrue(cluster.getBrokers().size() == 3);

            // execute kafka associated functions

        }
    }

    private void testKafkaFunctionality(String bootstrapServers, int partitions, int rf) throws Exception {
        try (
                AdminClient adminClient = AdminClient.create(
                        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));

                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        ImmutableMap.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        ImmutableMap.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "tc-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"
                        ),
                        new StringDeserializer(),
                        new StringDeserializer()
                );
        ) {
            String topicName = "messages";
            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, partitions, (short) rf));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            // let consumer side subscribe the given topic by name
            consumer.subscribe(Collections.singletonList(topicName));

            // generate record and send to brokers
            producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz")).get();

            Unreliables.retryUntilTrue(
                    10,
                    TimeUnit.SECONDS,
                    () -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                        if (records.isEmpty()) {
                            // consumer not receive(consume) records return false
                            // the Unreliables#retryUntilTrue will keep on trying until return true
                            return false;
                        }

                        Assertions.assertTrue(!records.isEmpty());
                        Set<TopicPartition> partitionSet = records.partitions();
                        Assertions.assertTrue(CollectionUtils.isNotEmpty(partitionSet));
                        for (TopicPartition p : partitionSet) {
                            List<ConsumerRecord<String, String>> list = records.records(p);
                            Assertions.assertTrue(CollectionUtils.isNotEmpty(list));
                        }
                        return true;
                    }
            );
            LOG.info("#testKafkaFunctionality consumer un-subscribe...");
            consumer.unsubscribe();
        } // try
    }
}
