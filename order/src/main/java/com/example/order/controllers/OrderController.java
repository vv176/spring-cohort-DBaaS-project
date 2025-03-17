package com.example.order.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ppa.LogMagickClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.*;

import com.example.order.data.Order;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate();
    private static final String DATASTORE_BASE_URL = "http://localhost:8080/datastore";
    @Autowired
    private LogMagickClient logMagickClient;
    @PostMapping("/place")
    public void placeOrder(@RequestParam String orderId, @RequestBody Order order) throws IOException, JsonProcessingException {
        logMagickClient.log("Received placeOrder API call for " +
                orderId + " and " + objectMapper.writeValueAsString(order));
        try {
            /**
             * Some logic
             */
            String jsonOrder = objectMapper.writeValueAsString(order);
            String url = String.format("%s/db/%s/table/%s/put?key=%s",
                    DATASTORE_BASE_URL, "amazon", "orders", orderId);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<>(jsonOrder, headers);
            restTemplate.postForObject(url, request, Void.class);
        } catch (Exception e) {
            logMagickClient.log("Faced exception in placeOrder API: " + e.getMessage());
            throw e;
        }
        logMagickClient.log("Processed placeOrder API request successfully");
    }

    @GetMapping("/details")
    public Order getOrder(@RequestParam String orderId) throws IOException, JsonProcessingException {
        logMagickClient.log("Received getOrder request " +
                "for " + orderId);
        ResponseEntity<String> response = null;
        try {
            String url = String.format("%s/db/%s/table/%s/get?key=%s",
                    DATASTORE_BASE_URL, "amazon", "orders", orderId);
            response = restTemplate.getForEntity(url, String.class);
            logMagickClient.log("DBaaS responded with " +
                    response.getBody());
        } catch (Exception e) {
            logMagickClient.log("Faced exception in getOrder API: " + e.getMessage());
            throw e;
        }
        Order o = objectMapper.readValue(response.getBody(), Order.class);
        logMagickClient.log("Processed getOrder API request successfully, " +
                "Response: " + objectMapper.writeValueAsString(o));
        return o;
    }
}
