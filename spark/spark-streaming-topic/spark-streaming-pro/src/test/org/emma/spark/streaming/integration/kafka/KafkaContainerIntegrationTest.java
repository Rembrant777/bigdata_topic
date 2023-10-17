package org.emma.spark.streaming.integration.kafka;

import org.emma.spark.streaming.testcontainer.kafka.KafkaContainerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        }
    }
}
