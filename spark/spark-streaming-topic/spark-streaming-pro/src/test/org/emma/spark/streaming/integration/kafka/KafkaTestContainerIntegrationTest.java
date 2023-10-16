package org.emma.spark.streaming.integration.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.emma.spark.streaming.testcontainer.kafka.KafkaTestContainer;
import org.emma.spark.streaming.testcontainer.zk.ZookeeperTestContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.lifecycle.Startables;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

/**
 * Integrated Testing for Kafka Cluster
 */
@Testcontainers
public class KafkaTestContainerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestContainerIntegrationTest.class);

    private static final String TOPIC = "TestKafkaTopic";

    private static final String URL = "";

    private static final long POLL_INTERVAL_MS = 100L;

    private static final long POLL_TIMEOUT_MS = 10_000L;

    private static Network network;

    private static KafkaTestContainer kafkaTestContainer;
    private static ZookeeperTestContainer zookeeperTestContainer;

    @BeforeAll
    public static void confluentSetup() throws Exception {
        network = Network.newNetwork();
        Assertions.assertNotNull(network);

        zookeeperTestContainer = new ZookeeperTestContainer()
                .withNetwork(network);
        kafkaTestContainer =
                new KafkaTestContainer(zookeeperTestContainer.getInternalUrl());

        // wonderful here use Startables to bind zk and kafka together
//        Startables
//                .deepStart(Stream.of(zookeeperTestContainer, kafkaTestContainer))
//                .join();
        zookeeperTestContainer.start();
    }

    @AfterAll
    public static void confluentShutdown() throws Exception {
        LOG.info("#confluentShutdown ");
        kafkaTestContainer.stop();
        kafkaTestContainer.close();

        zookeeperTestContainer.stop();
        zookeeperTestContainer.close();

        network.close();
    }

    @Test
    public void testDemo() {
        Assertions.assertTrue(Objects.nonNull(zookeeperTestContainer));
        Assertions.assertTrue(zookeeperTestContainer.isHostAccessible());
        // Assertions.assertTrue(zookeeperTestContainer.isHealthy());

        // Assertions.assertTrue(Objects.nonNull(kafkaTestContainer) && kafkaTestContainer.isHostAccessible());
    }

    // ---
    private static AdminClient createAdminClient() {
        AdminClient ret = null;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaTestContainer.getBootstrapServers());

        ret = KafkaAdminClient.create(props);

        LOG.info("#createAdminClient create ret non-null status {}", Objects.nonNull(ret));
        return ret;
    }

    private static void createTopics() throws InterruptedException, ExecutionException {
        try (AdminClient adminClient = createAdminClient()) {

            // todo: modify to duplicated replication and partition numbers,
            //  and then to verify the downstream consumed spark rdd's partitions

            short replicationFactor = 1;
            int partitions = 1;

            LOG.info("#createTopic {}", TOPIC);
            adminClient
                    .createTopics(Collections.singletonList(new NewTopic(TOPIC, partitions, replicationFactor)))
                    .all().get();
        }
    }
}
