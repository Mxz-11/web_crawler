package com.crawler;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;

public class RateLimiter {
    private final ConcurrentHashMap<String, Long> lastRequestTime = new ConcurrentHashMap<>();
    private final long delayMs;

    public RateLimiter() {
        this(1000);
    }

    public RateLimiter(long delayMs) {
        this.delayMs = delayMs;
    }

    public void acquire(String url) {
        String host = getHost(url);
        if (host == null)
            return;

        lastRequestTime.compute(host, (k, lastTime) -> {
            long now = System.currentTimeMillis();
            if (lastTime == null) {
                return now;
            } else {
                long nextAllowed = lastTime + delayMs;
                if (now < nextAllowed) {
                    try {
                        Thread.sleep(nextAllowed - now);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return System.currentTimeMillis();
                } else {
                    return now;
                }
            }
        });
    }

    private String getHost(String url) {
        try {
            return new URI(url).getHost();
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
