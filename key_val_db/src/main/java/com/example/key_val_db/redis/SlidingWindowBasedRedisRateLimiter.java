package com.example.key_val_db.redis;

import com.example.key_val_db.datastore.KeyValDataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SlidingWindowBasedRedisRateLimiter {

    private final StringRedisTemplate redisTemplate;
    private static final int WINDOW_SIZE = 10; // Sliding window size in seconds
    private final Map<String, Integer> clientRateLimits = new ConcurrentHashMap<>();
    private final KeyValDataStore keyValDataStore;

    @Autowired
    public SlidingWindowBasedRedisRateLimiter(StringRedisTemplate redisTemplate, KeyValDataStore keyValDataStore)
            throws IOException {
        this.redisTemplate = redisTemplate;
        this.keyValDataStore = keyValDataStore;
        loadRateLimitsFromDB();
    }

    @Scheduled(fixedRate = 60000) // Refresh every min
    public void loadRateLimitsFromDB() throws IOException {
        System.out.println("refreshing the rate-limits");
        Map<String, Integer> limits = keyValDataStore.loadRateLimits();
        synchronized (clientRateLimits) {
            clientRateLimits.clear();
            clientRateLimits.putAll(limits);
        }
    }

    public boolean allowRequest(String client) {
        int limit = 0;
        synchronized (clientRateLimits) {
            limit = clientRateLimits.getOrDefault(client, 10);
        }
        ZSetOperations<String, String> zSetOps = redisTemplate.opsForZSet();
        long now = Instant.now().toEpochMilli();
        long windowStart = now - (WINDOW_SIZE * 1000);

        // Removes all elements in the sorted set stored at key with a score between min and max (inclusive).
        zSetOps.removeRangeByScore(client, 0, windowStart);

        // Returns the number of elements in the sorted set (ZSET) stored at key
        Long requestCount = zSetOps.zCard(client);
        if (requestCount != null && requestCount >= limit) {
            System.out.println("Dis-allowing request of " + client);
            return false;
        }

        // Add the new request timestamp
        zSetOps.add(client, String.valueOf(now), now);

        // Set TTL for automatic cleanup : if a client goes idle OR if there's a bug in our cleanup algorithm we wrote ourselves
        redisTemplate.expire(client, Duration.ofSeconds(WINDOW_SIZE));

        System.out.println("Allowing request of " + client);
        return true;
    }

}
