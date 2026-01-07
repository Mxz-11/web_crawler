package com.crawler;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class StorageService {
    private final String outputPath;

    public StorageService(String outputPath) {
        this.outputPath = outputPath;
    }

    /**
     * Appends the crawled content to the output file in a thread-safe manner.
     * Format: ############## url # timestamp ######### content
     * 
     * @param url     The source URL
     * @param content The page content
     */
    public synchronized void store(String url, String content) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputPath, true)))) {
            long timestamp = System.currentTimeMillis();
            out.println("############## " + url + " # " + timestamp + " #########");
            out.println(content);
            out.println();
        } catch (IOException e) {
            System.err.println("Failed to write to storage: " + e.getMessage());
        }
    }
}
