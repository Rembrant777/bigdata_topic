package org.emma.spark.streaming.integration.zk;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
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

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.TimeUnit;



public class ZookeeperTestContainerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperTestContainerIntegrationTest.class);

    private static ZookeeperTestContainer zookeeperTestContainer;
    private static Network network;

    @BeforeAll
    public static void confluentSetup() throws Exception {
        LOG.info("#confluentSetup ...");

        network = Network.newNetwork();
        zookeeperTestContainer = new ZookeeperTestContainer()
                .withAccessToHost(true)
                .withNetwork(network);
        zookeeperTestContainer.start();
    }

    @AfterAll
    public static void confluentShutdown() throws Exception {
        LOG.info("#confluentShutdown ...");

        Assertions.assertNotNull(zookeeperTestContainer);
        zookeeperTestContainer.stop();
        zookeeperTestContainer.close();

        Assertions.assertNotNull(network);
        network.close();
    }

    @AfterAll
    public static void confluentTeardown() throws Exception {
        LOG.info("#confluentTeardown shutdown containers and network");
        zookeeperTestContainer.close();
        network.close();
    }

    /**
     * This test case try to verify whether the testcontainer of zookeeper can be setup successfully.
     */
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

    /**
     * In this test case try to execute basic zookeeper test case which copied from:
     * com.example.ZookeeperContainerTest#test
     */
    @Test
    public void zookeeperTestCase() {
        String path = "/message/zk-tc";
        String content = "Running Zookeeper with Testcontainers";

        // verify whether the zk container instance is still available
        Assertions.assertNotNull(zookeeperTestContainer);

        // wait until zk host is accessible in 10 seconds
        Unreliables.retryUntilTrue(10 * 1000,
                TimeUnit.SECONDS,
                () -> {
                    zookeeperTestContainer.getContainerInfo();
                    return zookeeperTestContainer.isHostAccessible();
                });

        String connectionStr = zookeeperTestContainer.getConnectString();
        Assertions.assertTrue(Objects.nonNull(connectionStr) && connectionStr.length() > 0);
        LOG.info("#zookeeperTestCase zk works on {}", connectionStr);

        // create zk client and attempt get connection to the zookeeper
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .builder()
                .connectString(connectionStr)
                .retryPolicy(new RetryOneTime(100))
                .build();

        curatorFramework.start();

        Assertions.assertNotNull(curatorFramework);
        Assertions.assertTrue(curatorFramework.isStarted());

        byte [] byteContent = null;

        try {
            curatorFramework.create().creatingParentsIfNeeded().forPath(path, content.getBytes(StandardCharsets.UTF_8));
            CuratorZookeeperClient client = curatorFramework.getZookeeperClient();
            System.out.println(client.isConnected());
            byteContent = curatorFramework.getData().forPath(path);
        } catch (Exception e) {
            LOG.error("#zookeeperTestCase create path on zk failed, caused by ", e);
        } finally {
            LOG.info("#zookeeperTestCase close client connection to zk server");
            curatorFramework.close();
            // confirm here whether the data write to zk path is equal to the zk specific retrieved data
            // curator 4.20 works fine with zk 3.4.x according to
            // https://curator.apache.org/zk-compatibility-34.html
            Assertions.assertTrue(new String(byteContent, StandardCharsets.UTF_8).equals(content));
        }
    }
}
