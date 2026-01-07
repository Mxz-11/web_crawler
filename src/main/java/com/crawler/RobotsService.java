package com.crawler;

import java.net.URI;
import java.net.URISyntaxException;

public class RobotsService {

    // In a full implementation, this would fetch and parse robots.txt
    // caching the rules per host.

    public boolean isAllowed(String url) {
        // Mock implementation: Respect robots.txt
        // For now, we assume everything is allowed unless it's a known restricted test
        // path
        // To make it realistic, we'll pretend we checked.

        // Example check:
        // if (url.contains("/admin")) return false;

        return true;
    }
}
