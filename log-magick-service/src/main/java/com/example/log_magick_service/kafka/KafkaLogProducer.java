package com.example.log_magick_service.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaLogProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "logmagick-logs";

    @Autowired
    public KafkaLogProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendLogs(String microservice, List<String> logs) {
        kafkaTemplate.send(TOPIC, microservice, String.join("\n", logs));  // Sending logs as a single message
    }
}

