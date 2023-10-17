package org.emma.spark.streaming.integration.zk;

import org.emma.spark.streaming.testcontainer.zk.ZookeeperTestContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.util.concurrent.TimeUnit;


public class ZookeeperTestContainerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperTestContainerIntegrationTest.class);

    private static ZookeeperTestContainer zookeeperTestContainer;
    private static Network network;

    @BeforeAll
    public static void confluentSetup() throws Exception {
        network = Network.newNetwork();
        zookeeperTestContainer = new ZookeeperTestContainer()
                .withAccessToHost(true)
                .withNetwork(network);
        zookeeperTestContainer.start();
    }

    @AfterAll
    public static void confluentTeardown() throws Exception {
        LOG.info("#confluentTeardown shutdown containers and network");
        zookeeperTestContainer.close();
        network.close();
    }

    @Test
    public void testSetup() {
        String TARGET_ZK_INTERNAL_URL = "zookeeper:2181";
        Assertions.assertNotNull(zookeeperTestContainer);
        Assertions.assertTrue(zookeeperTestContainer.isCreated());

        // wait until zk host is accessible in 10 seconds
        Unreliables.retryUntilTrue(10 * 1000,
                TimeUnit.SECONDS,
                () -> {
            zookeeperTestContainer.getContainerInfo();
                    return zookeeperTestContainer.isHostAccessible();
        });


        String zkInternalUrl = zookeeperTestContainer.getInternalUrl();
        Assertions.assertTrue(StringUtils.isNotBlank(zkInternalUrl)
                && zkInternalUrl.equals(TARGET_ZK_INTERNAL_URL));
    }
}
