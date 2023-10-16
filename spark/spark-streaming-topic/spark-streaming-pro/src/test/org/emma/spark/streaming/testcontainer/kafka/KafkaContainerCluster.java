package org.emma.spark.streaming.testcontainer.kafka;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * KafkaContainerCluster provides an eaay wao to lanuch Kafka Cluster with multiple brokers.
 */
public class KafkaContainerCluster implements Startable {
    // how many brokers in total to start
    private final int brokersNum;

    // kafka cluster network
    private final Network network;

    private final GenericContainer<?> zookeeper;

    private final Collection<KafkaContainer> brokers;

    public KafkaContainerCluster(String confluentPlatformVersion, int brokersNum, int internalTopicsRf) {
        if (brokersNum < 0) {
            throw new IllegalStateException("brokerNum '" + brokersNum  + "' must be greater than 0");
        }

        if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
            throw new IllegalArgumentException(
                    "internalTopicRf '" + internalTopicsRf + "' must be less than brokersNum and greater than 0");
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        this.zookeeper =
                new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper").withTag(confluentPlatformVersion))
                        .withNetwork(network)
                        .withNetworkAliases("zookeeper")
                        .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(KafkaContainer.ZOOKEEPER_PORT));

        this.brokers =
                IntStream
                        .range(0, this.brokersNum)
                        .mapToObj(brokerNum -> {
                            return new KafkaContainer(
                                    DockerImageName.parse("confluentinc/cp-kafka").withTag(confluentPlatformVersion))
                                    .withNetwork(this.network)
                                    .withNetworkAliases("broker-" + brokersNum)
                                    .dependsOn(this.zookeeper)
                                    .withExternalZookeeper("zookeeper:" + KafkaContainer.ZOOKEEPER_PORT)
                                    .withEnv("KAFKA_BROKER_ID", brokersNum + "")
                                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                                    .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                                    .withEnv("KAFKA_TRANSACTION_STATE_MIN_ISR", internalTopicsRf + "")
                                    .withStartupTimeout(Duration.ofMinutes(1));
                        })
                        .collect(Collectors.toList());

    }

    public Collection<KafkaContainer> getBrokers() {
        return this.brokers;
    }

    public String getBootstrapServers() {
        return brokers.stream().map(KafkaContainer::getBootstrapServers)
                .collect(Collectors.joining(","));
    }

    private Stream<GenericContainer<?>> allContainers() {
        return Stream.concat(this.brokers.stream(), Stream.of(this.zookeeper));
    }

    @Override
    public void start() {
        // sequential start to avoid resource contention on CI systems with weaker hardware
        brokers.forEach(GenericContainer::start);

        Unreliables.retryUntilTrue(
                30,
                TimeUnit.SECONDS,
                () -> {
                    Container.ExecResult result =
                            this.zookeeper.execInContainer();

                    String brokers = result.getStdout();

                    return brokers != null && brokers.split(",").length == this.brokersNum;
                }
        );
    }

    @Override
    public void stop() {
        allContainers().parallel().forEach(GenericContainer::stop);
    }
}
