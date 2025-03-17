package com.example.log_magick_service.controller;

import com.example.log_magick_service.kafka.KafkaLogProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/logs")
public class LogMagickController {
    @Autowired
    private KafkaLogProducer kafkaLogProducer;

    @PostMapping("/ingest")
    public String receiveLogs(@RequestBody Map<String, Object> payload) {
        String microserviceName = (String) payload.get("microserviceName");
        List<String> logs = (List<String>) payload.get("logs");

        if (microserviceName == null || logs == null || logs.isEmpty()) {
            return "Invalid request: microserviceName and logs are required.";
        }

        kafkaLogProducer.sendLogs(microserviceName, logs);
        return "Logs received and pushed to Kafka.";
    }
}
