package com.crawler;

import java.util.concurrent.ConcurrentHashMap;

public class VisitedManager {
    private final ConcurrentHashMap<String, Boolean> visitedUrls = new ConcurrentHashMap<>();

    /**
     * Checks if the URL has been visited. If not, marks it as visited and returns
     * true.
     * If already visited, returns false.
     * This operation is atomic.
     * 
     * @param url The URL to check
     * @return true if the URL was successfully marked as visited (first time),
     *         false otherwise.
     */
    public boolean shouldVisit(String url) {
        return visitedUrls.putIfAbsent(url, true) == null;
    }

    public int getVisitedCount() {
        return visitedUrls.size();
    }
}
