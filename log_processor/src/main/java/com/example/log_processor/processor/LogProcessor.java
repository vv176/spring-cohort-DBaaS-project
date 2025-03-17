package com.example.log_processor.processor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Service
public class LogProcessor {

    private static final String TOPIC_NAME = "logmagick-logs";
    private final KafkaConsumer<String, String> consumer;

    private static final String KEYVAL_DB_URL = "http://localhost:8080/datastore/{client}/table/{table}/putLogs";

    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    public LogProcessor() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "log-magick-group");
        props.setProperty("enable.auto.commit", "false"); // Manual commit for batch processing
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        new Thread(this::processLogs).start(); // Start log processing thread
    }

    private void processLogs() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) continue;

            try {
                for (ConsumerRecord<String, String> record : records) {
                    String logEntry = record.value();

                    // Extract microservice name from key
                    String microserviceName = record.key();

                    // Store log in KeyValDataStore
                    process(microserviceName, logEntry);
                }

                consumer.commitSync(); // Manually commit offsets after processing batch
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void process(String microserviceName, String logs) {
        if (logs.isEmpty()) return;

        String tableName = microserviceName + "_log";

        try {
            // Prepare headers
            HttpHeaders headers = new HttpHeaders();
            headers.set("Content-Type", "application/json");

            // Create request entity
            HttpEntity<String> requestEntity = new HttpEntity<>(logs, headers);

            // Call the KeyVal DB putLogs API
            ResponseEntity<Void> response = restTemplate.exchange(
                    KEYVAL_DB_URL, HttpMethod.POST, requestEntity, Void.class, microserviceName, tableName
            );

            if (!response.getStatusCode().is2xxSuccessful()) {
                System.err.println("LogProcessor: Failed to store logs for " + microserviceName);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("LogProcessor: Error while sending logs for " + microserviceName);
        }
    }

}

