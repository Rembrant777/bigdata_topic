package org.emma.spark.streaming.redis;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;

import java.util.Objects;
import java.util.Optional;

/**
 * An implementation of {@link Cache} that stores data in Redis.
 */
public class RedisBackendCache implements Cache{
    private final Jedis jedis;

    private final String cacheName;

    private final Gson gson;

    public RedisBackendCache(Jedis jedis, String cacheName) {
        this.jedis = jedis;
        this.cacheName = cacheName;
        this.gson = new Gson();
    }

    @Override
    public void put(String key, Object value) {
        String jsonValue = gson.toJson(value);
        this.jedis.hset(this.cacheName, key, jsonValue);
    }

    @Override
    public <T> Optional<T> get(String key, Class<T> expectedClass) {
        String foundJson = this.jedis.hget(this.cacheName, key);
        if (Objects.isNull(foundJson)) {
            return Optional.empty();
        }
        return Optional.of(gson.fromJson(foundJson, expectedClass));
    }
}
