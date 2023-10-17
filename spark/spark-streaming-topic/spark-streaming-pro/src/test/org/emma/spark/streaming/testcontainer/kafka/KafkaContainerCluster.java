package org.emma.spark.streaming.testcontainer.kafka;

import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * KafkaContainerCluster provides an easy way to launch Kafka Cluster with multiple brokers.
 */
public class KafkaContainerCluster implements Startable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerCluster.class);
    private static final int ZOOKEEPER_TICK_TIME = 2000;

    // how many brokers in total to start
    private final int brokersNum;

    // kafka cluster network
    private final Network network;

    private final GenericContainer<?> zookeeper;

    private final Collection<KafkaContainer> brokers;

    public KafkaContainerCluster(String confluentPlatformVersion, int brokersNum, int internalTopicsRf) {
        if (brokersNum < 0) {
            throw new IllegalStateException("brokerNum '" + brokersNum + "' must be greater than 0");
        }

        if (internalTopicsRf < 0 || internalTopicsRf > brokersNum) {
            throw new IllegalArgumentException(
                    "internalTopicRf '" + internalTopicsRf + "' must be less than brokersNum and greater than 0");
        }

        this.brokersNum = brokersNum;
        this.network = Network.newNetwork();

        this.zookeeper =
                new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper")
                        .withTag(confluentPlatformVersion))
                        .withNetwork(network)
                        .withNetworkAliases("zookeeper")
                        .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(KafkaContainer.ZOOKEEPER_PORT))
                        .withEnv("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME))
                        // here we attempt to force set the docker image's platform to linux/amd64
                        // to avoid the in-compatibility with MacOs Apple M2 Max
                        .withEnv("DOCKER_DEFAULT_PLATFORM", "linux/amd64");

        this.brokers =
                IntStream.range(0, this.brokersNum)
                        .mapToObj(brokerNum -> {
                            LOG.info("#KafkaContainer init broker total num {}, broker no. {}, replica cnt {}",
                                    brokersNum, brokerNum, internalTopicsRf);

                            KafkaContainer kafkaContainer = initKafkaContainer(confluentPlatformVersion,
                                    brokerNum, internalTopicsRf);

                            LOG.info("#KafkaContainer init non-null status {}", Objects.nonNull(kafkaContainer));
                            return kafkaContainer;
                        })
                        .collect(Collectors.toList());
    }

    /**
     * Method to create KafkaContainer instance by provided parameters.
     *
     * @param confluentPlatformVersion zk && kafka versions of confluent platform released image.
     * @param brokerNum                number of the brokers
     * @param internalTopicsRf         replica count of each partition that stores on the kafka brokers.
     * @return instance of {@link KafkaContainer}
     */
    private KafkaContainer initKafkaContainer(String confluentPlatformVersion, int brokerNum, int internalTopicsRf) {
        KafkaContainer ret = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")
                .withTag(confluentPlatformVersion))
                .withNetwork(this.network)
                .withNetworkAliases("broker-" + brokerNum)
                .dependsOn(this.zookeeper)
                .withExternalZookeeper("zookeeper:" + KafkaContainer.ZOOKEEPER_PORT)
                .withEnv("KAFKA_BROKER_ID", brokerNum + "")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                .withEnv("KAFKA_TRANSACTION_STATE_MIN_ISR", internalTopicsRf + "")
                // here we attempt to force set the docker image's platform to linux/amd64
                // to avoid the in-compatibility with MacOs Apple M2 Max
                .withEnv("DOCKER_DEFAULT_PLATFORM", "linux/amd64")
               // .withEnv("KAFKA_PORT", String.valueOf(genExposePort(brokerNum + 3)))
                // .withExposedPorts(genExposePort(brokerNum + 3))
                .withStartupTimeout(Duration.ofMinutes(1));

        LOG.info("#initKafkaContainer ret non-null status {}", Objects.nonNull(ret));
        return ret;
    }

    /**
     *
     */
    private Integer genExposePort(int brokersNum) {
        int kafkaBrokerExposePort = Integer.valueOf(String.format("909%s", brokersNum));
        LOG.info("#genExposePort {}", kafkaBrokerExposePort);
        return kafkaBrokerExposePort;
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
        zookeeper.start();

        // here check zookeeper
        Unreliables.retryUntilTrue(20,
                TimeUnit.SECONDS,
                () -> {
                    return zookeeper.isRunning();
                });

        // when zk setup and running then setup kafka containers one by one
        brokers.forEach(GenericContainer::start);
    }

    @Override
    public void stop() {
        allContainers().parallel().forEach(GenericContainer::stop);
    }



    /**
     * Method to verify whether all brokers in the kafka cluster are in the state of running.
     */
    public boolean isAllBrokerRunning() {
        return this.brokers.stream()
                .parallel()
                .map(this::isBrokerRunning)
                .collect(Collectors.toList())
                .size() == this.brokersNum;
    }
    /**
     * Method to verify whether the kafka broker is in started && running state.
     */
    private boolean isBrokerRunning(KafkaContainer kafkaContainer) {
        return Objects.nonNull(kafkaContainer) && kafkaContainer.isRunning();
    }

    /**
     * Method to verify whether all brokers in the kafka cluster are in the state of healthy state.
     *
     * Kafka Broker does not have the check heal api this method will be set deprecated.
     */
    @Deprecated
    public boolean isAllBrokerHealth() {
        return this.brokers.stream()
                .parallel()
                .map(this::isBrokerHealth)
                .collect(Collectors.toList())
                .size() == this.brokersNum;
    }

    /**
     * Method to verify whether the kafka broker is in started && running and also in healthy state.
     * Kafka Broker does not have the check heal api this method will be set deprecated.
     */
    @Deprecated
    private boolean isBrokerHealth(KafkaContainer kafkaContainer) {
        return Objects.nonNull(kafkaContainer) && kafkaContainer.isHealthy();
    }
}
