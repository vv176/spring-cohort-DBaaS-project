package com.example.key_val_db.controllers;

import com.example.key_val_db.datastore.KeyValDataStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.*;

import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

@RestController
@RequestMapping("/datastore")
public class DataStoreController {
    @Autowired
    private KeyValDataStore keyValueStore;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @PostMapping("/db/{client}")
    public void createDatabase(@PathVariable String client) {
        keyValueStore.createDB(client);
    }

    @PostMapping("/db/{client}/table/{table}")
    public void createTable(@PathVariable String client, @PathVariable String table) throws IOException, IOException {
        keyValueStore.createTable(client, table);
    }
    // rate-limiting : AMZN : 3 req in 10 secs
    @PostMapping("/db/{client}/table/{table}/put")
    public void putValue(@PathVariable String client, @PathVariable String table, @RequestParam String key, @RequestBody String value) throws IOException {
        //System.out.println("request:" + client + "|" + table + "|" + key + "|" + value);
        keyValueStore.put(key, value, client, table);
        // Invalidate Redis Cache
        String cacheKey = client + ":" + table + ":" + key;
        redisTemplate.delete(cacheKey);
    }

    @GetMapping("/db/{client}/table/{table}/get")
    public String getValue(@PathVariable String client, @PathVariable String table, @RequestParam String key) throws IOException {
        String cacheKey = client + ":" + table + ":" + key;

        // 1. Check Redis Cache
        String cachedValue = redisTemplate.opsForValue().get(cacheKey);
        if (cachedValue != null) {
            System.out.println("Cache hit for key: " + cacheKey);
            return cachedValue;
        }

        String value = keyValueStore.get(key, client, table);
        if (value != null) {
            // 3. Store in Redis for future requests (with TTL of 10 mins)
            redisTemplate.opsForValue().set(cacheKey, value,
                    Duration.ofMinutes(10));
        }
        return value;
    }

    @DeleteMapping("/db/{client}")
    public void deleteDatabase(@PathVariable String client) {
        keyValueStore.deleteDB(client);
    }

    @DeleteMapping("/db/{client}/table/{table}")
    public void deleteTable(@PathVariable String client, @PathVariable String table) {
        keyValueStore.deleteTable(client, table);
    }

    @PostMapping("/{client}/table/{table}/putLogs")
    public void putLogs(@PathVariable String client, @PathVariable String table, @RequestBody String data) throws IOException {
        for (String logEntry : data.split("\n")) {
            int spaceIndex = logEntry.indexOf(' '); // Extract timestamp
            if (spaceIndex == -1) continue; // Skip invalid logs

            String timestamp = logEntry.substring(0, spaceIndex); // Key
            String logMessage = logEntry.substring(spaceIndex + 1); // Value

            putValue(client, table, timestamp, logMessage);
        }
    }
}
