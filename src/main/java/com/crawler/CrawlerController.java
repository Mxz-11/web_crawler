package com.crawler;

import java.util.List;
import java.util.concurrent.*;
import java.io.IOException;

public class CrawlerController {
    private final BlockingQueue<String> urlQueue;
    private final ExecutorService executorService;
    private final VisitedManager visitedManager;
    private final RateLimiter rateLimiter;
    private final StorageService storageService;
    private final int numThreads;
    private volatile boolean isRunning = true;

    public CrawlerController(List<String> seeds, int maxPages, int numThreads) {
        this.urlQueue = new LinkedBlockingQueue<>();
        this.visitedManager = new VisitedManager();
        this.rateLimiter = new RateLimiter();
        this.storageService = new StorageService("crawled_data.txt");
        this.numThreads = numThreads;
        this.executorService = Executors.newFixedThreadPool(numThreads);

        for (String seed : seeds) {
            urlQueue.offer(seed);
        }
    }

    public void start() {
        try {
            for (int i = 0; i < numThreads; i++) {
                executorService.submit(new WorkerTask(urlQueue, visitedManager, rateLimiter, storageService));
            }

            // In a real application, you might want a more sophisticated way to stop
            // For this exercise, we depend on the user terminating or adding a max-page
            // logic inside workers/controller
            // But we will add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

            // Allow the crawler to run for some time or until queue is empty (if we decide
            // to stop on empty)
            // For now, let's keep it running.
            // We can also await termination if we had a specific stop condition.

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        System.out.println("Shutting down crawler...");
        isRunning = false;
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
