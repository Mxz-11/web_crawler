package com.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class WorkerTask implements Runnable {
    private final BlockingQueue<String> urlQueue;
    private final VisitedManager visitedManager;
    private final RateLimiter rateLimiter;
    private final StorageService storageService;
    private final RobotsService robotsService = new RobotsService();

    public WorkerTask(BlockingQueue<String> urlQueue, VisitedManager visitedManager, RateLimiter rateLimiter,
            StorageService storageService) {
        this.urlQueue = urlQueue;
        this.visitedManager = visitedManager;
        this.rateLimiter = rateLimiter;
        this.storageService = storageService;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                String url = urlQueue.poll(500, TimeUnit.MILLISECONDS);
                if (url == null) {
                    continue;
                }

                if (!visitedManager.shouldVisit(url)) {
                    continue;
                }

                if (!robotsService.isAllowed(url)) {
                    System.out.println("Disallowed by robots.txt: " + url);
                    continue;
                }

                rateLimiter.acquire(url);

                System.out.println("Fetching: " + url + " [Thread: " + Thread.currentThread().getName() + "]");
                try {
                    Document doc = Jsoup.connect(url)
                            .userAgent("JavaCrawler/1.0")
                            .timeout(5000)
                            .get();

                    storageService.store(url, doc.outerHtml());

                    Elements links = doc.select("a[href]");
                    for (Element link : links) {
                        String absUrl = link.attr("abs:href");
                        if (absUrl != null && !absUrl.isEmpty() && absUrl.startsWith("http")) {
                            urlQueue.offer(absUrl);
                        }
                    }

                } catch (IOException e) {
                    System.err.println("Error fetching " + url + ": " + e.getMessage());
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.err.println("Unexpected error in worker: " + e.getMessage());
            }
        }
    }
}
