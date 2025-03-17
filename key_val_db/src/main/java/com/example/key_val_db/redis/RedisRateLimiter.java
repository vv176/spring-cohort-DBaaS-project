package com.example.key_val_db.redis;

import com.example.key_val_db.datastore.KeyValDataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RedisRateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final Map<String, Integer> clientRateLimits = new ConcurrentHashMap<>();
    private final KeyValDataStore keyValDataStore;

    @Autowired
    public RedisRateLimiter(StringRedisTemplate redisTemplate, KeyValDataStore dataStore) throws IOException {
        this.redisTemplate = redisTemplate;
        this.keyValDataStore = dataStore;
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

    // Check if the client is within the rate limit
    public boolean allowRequest(String client) {
        int limit = 0;
        synchronized (clientRateLimits) {
            limit = clientRateLimits.getOrDefault(client, 10);
        }
        Long currentCount = redisTemplate.opsForValue().increment(client, 1);
        if (currentCount == 1) {
            redisTemplate.expire(client, Duration.ofSeconds(10)); // Reset after 10 secs
        }
        boolean response = currentCount <= limit;
        if (response)
            System.out.println("Allowing the request of " + client);
        else
            System.out.println("Dis-allowing the request of " + client);
        return response;
    }

}
