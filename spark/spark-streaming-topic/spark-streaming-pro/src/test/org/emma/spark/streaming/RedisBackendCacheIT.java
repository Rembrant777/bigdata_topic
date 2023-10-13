package org.emma.spark.streaming;

import org.apache.directory.api.util.Strings;
import org.emma.spark.streaming.redis.Cache;
import org.emma.spark.streaming.redis.RedisBackendCache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import java.util.Optional;

/**
 * Integrated Testing for RedisBackendCache
 */
@Testcontainers
public class RedisBackendCacheIT {
    private static final Logger LOG = LoggerFactory.getLogger(RedisBackendCacheIT.class);
    private Cache cache;

    private static ThreadLocal<Jedis> jedis;

    @Container
    public static GenericContainer<?> redis = new GenericContainer<>(DockerImageName
            .parse("redis:3.0.6"))
            .withExposedPorts(6379);

    @BeforeClass
    public static void startContainer() {
        LOG.info("#startContainer ...");
        redis.start();
    }

    @AfterClass
    public static void stopContainer() {
        LOG.info("#stopContainer ...");
        redis.stop();
    }


    @BeforeAll
    public static void setUp() {
        jedis = ThreadLocal.withInitial(() -> {
            return null;
        });
    }

    @BeforeEach
    public void init() {
        assertTrue(Strings.isNotEmpty(redis.getHost()));
        assertTrue(redis.getMappedPort(6379) > 0);
        cache = new RedisBackendCache(this.jedis.get(), "testcontainer redis");
        assertNotNull(cache);
    }

    @AfterAll
    public static void shutDown() {
        if (jedis.get() != null && jedis.get().isConnected()) {
            jedis.get().close();
        }
    }

    @Test
    public void testFindingAndInsertedValue() {
        cache.put("foo", "FOO");
        Optional<String> foundObject = cache.get("foo", String.class);

        assertThat(foundObject.isPresent()).as("When an object in the cache is retrieved, it can be found").isTrue();
        assertThat(foundObject.get())
                .as("When we put a String in to the cache and retrieve it, the value is the same")
                .isEqualTo("FOO");
    }

    @Test
    public void testnotFindingAValueThatWasNotInserted() {
        Optional<String> foundObject = cache.get("bar", String.class);

        assertThat(foundObject.isPresent())
                .as("When an object that's not in the cache is retrieved, nothing is found")
                .isFalse();
    }
}
