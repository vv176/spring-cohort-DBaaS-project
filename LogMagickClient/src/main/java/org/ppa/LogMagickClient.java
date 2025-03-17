package org.ppa;
import com.google.gson.Gson;

import java.util.concurrent.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class LogMagickClient {
    private static final int BATCH_SIZE = 50;
    private static final int FLUSH_INTERVAL = 15000; // 15 seconds
    private static final String LOGMAGICK_SERVER_URL = "http://localhost:8082/logs/ingest";

    private final Queue<String> logQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final String microserviceName;

    public LogMagickClient(String microserviceName) {
        this.microserviceName = microserviceName;
        scheduler.scheduleAtFixedRate(this::flushLogs, FLUSH_INTERVAL, FLUSH_INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    public void log(String logMessage) {
        String logEntry = System.currentTimeMillis() + " " + logMessage;
        logQueue.add(logEntry);
        if (logQueue.size() >= BATCH_SIZE) {
            flushLogs();
        }
    }

    private synchronized void flushLogs() {
        if (logQueue.isEmpty()) return;

        List<String> batch = new ArrayList<>();
        while (!logQueue.isEmpty() && batch.size() < BATCH_SIZE) {
            batch.add(logQueue.poll());
        }

        sendToServer(batch);
    }

    private void sendToServer(List<String> logs) {
        try {
            URL url = new URL(LOGMAGICK_SERVER_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            // Prepare the JSON payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("microserviceName", microserviceName);
            payload.put("logs", logs);
            String jsonPayload = new Gson().toJson(payload);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonPayload.getBytes());
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("LogMagickClient: Failed to send logs, response: " + responseCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        scheduler.shutdown();
        flushLogs();
    }
}

