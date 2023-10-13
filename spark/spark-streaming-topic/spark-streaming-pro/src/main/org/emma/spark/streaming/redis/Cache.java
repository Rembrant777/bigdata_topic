package org.emma.spark.streaming.redis;

import java.util.Optional;

/**
 * Define Cache basic operations for storing data associated with keys.
 */
public interface Cache {
    /**
     * Store a value object in the cache with no specific expiry time.
     * The object maybe evicted by the cache any time if necessary.
     *
     * @param key key that may be used to retrieve the object in the future.
     * @param value the value object to be stored in redis
     */
    void put(String key, Object value);

    /**
     * Retrieve a value object from the cache.
     * @param key the key that was used to insert the object initially.
     * @param expectedClass for convenience, a class that the object should be cast to before being returned.
     * @param <T> the class of the returned object
     * @return the object if it was in the cache, or an empty Optinal if not found
     */
    <T> Optional<T> get(String key, Class<T> expectedClass);
}
