package org.emma.spark.streaming.integration.kafka;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integrated Testing for Kafka Cluster
 */
@Testcontainers
public class KafkaBackendIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBackendIntegrationTest.class);

    @Container
    public static GenericContainer<?> zk = new GenericContainer<>(DockerImageName.parse(""))
            .withExposedPorts();

    @Container
    public static GenericContainer<?> kafka = new GenericContainer<>(DockerImageName.parse(""))
            .withExposedPorts();

    @BeforeClass
    public static void startContainers() {
        LOG.info("#startContainers start containers");
        zk.start();
        kafka.start();
    }

    @AfterClass
    public static void stopContainers() {
        LOG.info("#stopContainers stop containers");
        zk.stop();
        kafka.stop();
    }
}
