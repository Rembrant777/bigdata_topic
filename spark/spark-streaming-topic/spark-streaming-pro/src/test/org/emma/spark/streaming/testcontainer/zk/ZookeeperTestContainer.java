package org.emma.spark.streaming.testcontainer.zk;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.io.IOException;
import java.util.Map;

import static org.emma.spark.streaming.constants.Constants.CONFLUENT_PLATFORM_VERSION;

/**
 * Customized implemented TestContainer based on abstract class {@link GenericContainer<>}
 */
public class ZookeeperTestContainer extends GenericContainer<ZookeeperTestContainer> {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperTestContainer.class);

    private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
    private static final int ZOOKEEPER_TICK_TIME = 2000;

    private final String networkAlias = "zookeeper";

    public ZookeeperTestContainer() throws IOException {
        this(CONFLUENT_PLATFORM_VERSION);
    }

    public ZookeeperTestContainer(String confluentPlatformVersion) throws IOException {
        super(getZookeeperContainerImage(confluentPlatformVersion));

        Map<String, String> env = Maps.newHashMap();
        // export env properties
        env.put("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_INTERNAL_PORT));
        env.put("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME));
        withEnv(env);

        addExposedPort(ZOOKEEPER_INTERNAL_PORT);
        withNetworkAliases(networkAlias);
    }

    public String getInternalUrl() {
        return String.format("%s:%d", networkAlias, ZOOKEEPER_INTERNAL_PORT);
    }

    private static String getZookeeperContainerImage(String confluentPlatformVersion) {
        String zkImageName =  (String) TestcontainersConfiguration
                .getInstance()
                .getProperties()
                .getOrDefault(
                        "zookeeper.container.image",
                        // todo:  --platform linux/amd64
                        "confluentinc/cp-zookeeper:" + confluentPlatformVersion);
        LOG.info("#getZookeeperContainerImage zkImageName {}", zkImageName);
        return zkImageName;
    }
}
