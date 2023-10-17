package org.emma.spark.streaming.integration.kafka;

import org.emma.spark.streaming.testcontainer.kafka.KafkaContainerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Add test cases for {@link org.emma.spark.streaming.testcontainer.kafka.KafkaContainerCluster},
 * which copied from testcontainer repository's
 * examples/kafka-cluster/src/test/java/com/example/kafkacluster/KafkaContainerClusterTest.java
 */
public class KafkaContainerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerIntegrationTest.class);

    @Test
    void testKafkaContainerCluster() throws Exception {
        int BROKER_NUM = 3;
        int TOPIC_REPLICATION_CNT = 2;

        try (KafkaContainerCluster cluster = new KafkaContainerCluster("6.2.1", BROKER_NUM, TOPIC_REPLICATION_CNT)) {
            cluster.start();

            Unreliables.retryUntilTrue(10 * 1000,
                    TimeUnit.SECONDS,
                    () -> {
                return StringUtils.isNotBlank(cluster.getBootstrapServers())
                    });

            String bootstrapServers = cluster.getBootstrapServers();
            Assertions.assertEquals(cluster.getBrokers().size(), BROKER_NUM);
        }
    }
}
