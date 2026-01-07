package com.crawler;

import java.util.Arrays;
import java.util.List;

public class App {
    public static void main(String[] args) {
        System.out.println("Starting Web Crawler...");

        List<String> seeds = Arrays.asList(
                "https://toscrape.com");

        CrawlerController controller = new CrawlerController(seeds, 10, 4);
        controller.start();
    }
}
