package org.emma.spark.streaming.testcontainer.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.emma.spark.streaming.constants.Constants.CONFLUENT_PLATFORM_VERSION;

/**
 * Test Container examples not support Kafka Container, try to implement one.
 */
public class KafkaTestContainer extends GenericContainer<KafkaTestContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTestContainer.class);

    private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    private static final int KAFKA_INTERNAL_PORT = 9091;
    private static final int KAFKA_INTERNAL_PORT_2 = 9092;
    private static final int KAFKA_INTERNAL_PORT_3 = 9093;

    public static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT = 29091;
    public static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT_2 = 29092;
    public static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT_3 = 29093;

    private static final int PORT_NOT_ASSIGNED = -1;

    private String zookeeperConnect = null;

    private int port = PORT_NOT_ASSIGNED;

    private final String networkAlias = "kafka";

    public KafkaTestContainer(String zookeeperConnect) {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public KafkaTestContainer(String confluentPlatformVersion, String zookeeperConnect) {
        super(getKafkaContainerImage(confluentPlatformVersion));

        this.zookeeperConnect = zookeeperConnect;
        withExposedPorts(KAFKA_INTERNAL_PORT);

        // use two listeners with different names, it will force Kafka to communicate with itself via internal
        // listener when KAFKA_INTER_BROKER_LISTENER_NAME is set.
        // otherwise Kafka will try to use the advertised listener.
        withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT
                + ",BROKER://0.0.0.0:" + KAFKA_INTERNAL_PORT);
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

        withEnv("KAFKA_BROKER_ID", "1");
        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
        withEnv("KAFKA_LOG_FLUSH_INTERNAL_MESSAGES", Long.MAX_VALUE + "");
        withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

        withNetworkAliases(networkAlias);
    }

    public String getBootstrapServers() {
        if (port == PORT_NOT_ASSIGNED) {
            throw new IllegalStateException("You should start Kafka container first");
        }

        return String.format("PLAINTEXT://%s:%s", getHost(), port);
    }

    @Override
    protected void doStart() {
        withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
        super.doStart();
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo);

        port = getMappedPort(KAFKA_INTERNAL_PORT);

        if (reused) {
            return;
        }

        String command = "#!/bin/bash \n";
        command += "export KAFKA_ZOOKEEPER_CONNECT='" + zookeeperConnect + "'\n";
        command += "export KAFKA_ADVERTISED_LISTENERS='" + Stream
                .concat(
                        Stream.of("PLAINTEXT://" + networkAlias + ":" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT),
                        containerInfo.getNetworkSettings().getNetworks().values().stream()
                                .map(it -> "BROKER://" + it.getIpAddress() + ":" + KAFKA_INTERNAL_PORT)
                ).collect(Collectors.joining(",")) + "\n";

        command += ". /etc/confluent/docker/bash-config \n";
        command += "/etc/confluent/docker/configure \n";
        command += "/etc/confluent/docker/launch \n";

        LOG.info("#containerIsStarting command {}", command);
        copyFileToContainer(
                Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                STARTER_SCRIPT);
    }

    private static String getKafkaContainerImage(String confluentPlatformVersoin) {
        return (String) TestcontainersConfiguration
                .getInstance().getUserProperties().getOrDefault(
                        "kafka.container.image",
                        "confluentinc/cp-kafka:" + confluentPlatformVersoin);
    }
}
