package com.example.key_val_db.datastore;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
@Component
public class KeyValDataStore {
    private static final String BASE_PATH = "/Users/vivekanandvivek/Downloads/key_val_db/src/main/resources/databases";
    private String dbName;
    private String tableName;
    private Map<String, RandomAccessFile> tableToFilePtr;
    private Map<String, Map<String, Long>> tableToIndex;

    public KeyValDataStore() throws IOException {
        this.tableToFilePtr = new HashMap<>();
        this.tableToIndex = new HashMap<>();

    }
    @PostConstruct
    public void init() throws IOException {
        traverseAndInitialize(new File(BASE_PATH), "");
    }
    private void traverseAndInitialize(File directory, String clientPath) throws IOException {
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                String relativePath = clientPath.isEmpty() ? file.getName() : clientPath + "/" + file.getName();
                traverseAndInitialize(file, relativePath);
            } else if (file.isFile() && file.getName().endsWith(".store")) {
                initializeTable(clientPath, BASE_PATH + "/" + clientPath + "/" + file.getName());
            }
        }
    }

    private void initializeTable(String url, String table) throws IOException {
        System.out.println(url + " , " + table);
        File tableFile = new File(table);
        RandomAccessFile file = new RandomAccessFile(tableFile, "rw");
        tableToFilePtr.put(url, file);
        tableToIndex.put(url, new HashMap<>());
        loadIndex(url, table);
    }

    private void loadIndex(String url, String table) throws IOException {
        RandomAccessFile file = tableToFilePtr.get(url);
        if (file == null) {
            throw new IllegalStateException("No file found while loading index for : " + url);
        }
        Map<String, Long> index = tableToIndex.get(url);
        long position = 0;
        file.seek(0);
        while (file.getFilePointer() < file.length()) {
            int keyLength = file.readInt();
            byte[] keyBytes = new byte[keyLength];
            file.readFully(keyBytes);
            String key = new String(keyBytes);

            int valueLength = file.readInt();
            file.skipBytes(valueLength);

            index.put(key, position);
            position = file.getFilePointer();
        }
    }
    //                    "abc"       "123"          "amazon"       "cart"
    //   "/amazon/cart" ===> f1
    //   "/uber/drivers" ===> f2
    public void put(String key, String value, String client, String table) throws IOException {
        String url = getURL(client, table);
        RandomAccessFile file = tableToFilePtr.get(url);
        if (file == null) {
            throw new IllegalArgumentException("Invalid combination of " +
                    client + " and " + table);
        }
        long position = file.length();
        file.seek(position);

        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value.getBytes();

        file.writeInt(keyBytes.length);
        file.write(keyBytes);
        file.writeInt(valueBytes.length);
        file.write(valueBytes);
        Map<String, Long> index = tableToIndex.get(url);
        if (index == null) {
            throw new IllegalStateException("No index found for : " + url);
        }
        index.put(key, position);
    }

    public String get(String key, String client, String table) throws IOException {
        String url = getURL(client, table);
        Map<String, Long> index = tableToIndex.get(url);
        if (index == null) {
            throw new IllegalArgumentException("No index found for: " + url);
        }
        Long position = index.get(key); // offset
        if (position == null) return null;
        RandomAccessFile file = tableToFilePtr.get(url);
        if (file == null) {
            throw new IllegalStateException("No file pointer for:" + url);
        }
        file.seek(position);
        int keyLength = file.readInt();
        file.skipBytes(keyLength);
        int valueLength = file.readInt();
        byte[] valueBytes = new byte[valueLength];
        file.readFully(valueBytes);

        return new String(valueBytes);
    }

    public void close() throws IOException {
        for (Map.Entry<String, RandomAccessFile> entry : tableToFilePtr.entrySet()) {
            entry.getValue().close();
        }
    }

    public void createDB(String client) {
        File dbDir = new File(BASE_PATH, client);
        if (!dbDir.exists()) dbDir.mkdirs();
    }

    public void deleteDB(String client) {
        File dbDir = new File(BASE_PATH, client);
        if (dbDir.exists()) deleteRecursive(dbDir);
    }

    public void createTable(String client, String table) throws IOException {
        File dbDir = new File(BASE_PATH + "/" + client);

        if (!dbDir.exists()) {
            throw new IllegalStateException("Client database directory does not exist: " + dbDir.getAbsolutePath());
        }

        File tableDir = new File(dbDir + "/" + table);
        if (!tableDir.exists()) {
            tableDir.mkdir();
        } else {
            return;
        }

        File tableFile = new File(tableDir + "/" + table + ".store");
        if (!tableFile.exists()) {
            tableFile.createNewFile();
        }

        RandomAccessFile file = new RandomAccessFile(tableFile, "rw");
        tableToFilePtr.put(getURL(client, table), file);
        tableToIndex.put(getURL(client, table), new HashMap<>());
    }


    public void deleteTable(String client, String table) {
        File tableFile = new File(BASE_PATH + "/" + client + "/" +
                table + "/" + table + ".store");
        if (tableFile.exists()) tableFile.delete();
        File tableFileDir = new File(BASE_PATH + "/" + client + "/" +
                table);
        tableFileDir.delete();
        tableToFilePtr.remove(getURL(client, table));
        tableToIndex.remove(getURL(client, table));
    }

    private void deleteRecursive(File file) {
        if (file.isDirectory()) {
            for (File subFile : file.listFiles()) {
                deleteRecursive(subFile);
            }
        }
        file.delete();
    }

    public Map<String, Integer> loadRateLimits() throws IOException {
        File rateLimitFile = new File("/Users/vivekanandvivek/Downloads/key_val_db/src/main/resources" + "/rate-limits/rate-limits.store");
        Map<String, Integer> rateLimits = new HashMap<>();

        if (!rateLimitFile.exists()) {
            throw new IllegalStateException("Rate limit file not found: " + rateLimitFile.getAbsolutePath());
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(rateLimitFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                if (parts.length == 2) {
                    String client = parts[0].trim();
                    Integer limit = Integer.parseInt(parts[1].trim());
                    rateLimits.put(client, limit);
                }
            }
        }
        return rateLimits;
    }

    private String getURL(String client, String table) {
        return client + "/" + table;
    }

}
