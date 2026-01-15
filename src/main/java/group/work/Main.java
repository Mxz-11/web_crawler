package group.work;

import java.util.Arrays;
import java.util.List;

import group.work.crawler.CrawlerController;

public class Main {
    public static void main(String[] args) {
        System.out.println("---WEB CRAWLER---");
        List<String> seeds = Arrays.asList("https://toscrape.com");
        int max_pages = 50;
        int num_threads = 10;
        CrawlerController controller = new CrawlerController(seeds, max_pages, num_threads);
        controller.start();
    }
}