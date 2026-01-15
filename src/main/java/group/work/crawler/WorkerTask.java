package group.work.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import group.work.handlers.HttpFetcher;
import group.work.handlers.RobotsService;
import group.work.handlers.ScopePolicy;
import group.work.helpers.RateLimiter;
import group.work.helpers.UrlNormalizer;
import group.work.helpers.VisitedManager;
import group.work.storage.StorageService;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerTask implements Callable<Void> {
    private final BlockingQueue<String> url_queue;
    private final VisitedManager visited_manager;
    private final RateLimiter rate_limiter;
    private final StorageService storage_service;
    private final RobotsService robots_service;
    private final HttpFetcher http_fetcher;
    private final ScopePolicy scope_policy;
    private final AtomicInteger pages_stored;
    private final AtomicInteger in_flight;
    private final AtomicBoolean stop_requested;
    private final int max_pages;

    public WorkerTask(
            BlockingQueue<String> url_queue,
            VisitedManager visited_manager,
            RateLimiter rate_limiter,
            StorageService storage_service,
            RobotsService robots_service,
            HttpFetcher http_fetcher,
            ScopePolicy scope_policy,
            AtomicInteger pages_stored,
            AtomicInteger in_flight,
            AtomicBoolean stop_requested,
            int max_pages) {
        this.url_queue = url_queue;
        this.visited_manager = visited_manager;
        this.rate_limiter = rate_limiter;
        this.storage_service = storage_service;
        this.robots_service = robots_service;
        this.http_fetcher = http_fetcher;
        this.scope_policy = scope_policy;
        this.pages_stored = pages_stored;
        this.in_flight = in_flight;
        this.stop_requested = stop_requested;
        this.max_pages = max_pages;
    }

    @Override
    public Void call() {
        while (!Thread.currentThread().isInterrupted() && !stop_requested.get()) {
            String url = null;
            boolean counted_in_flight = false;
            try {
                if (this.pages_stored.get() >= this.max_pages) {
                    this.stop_requested.set(true);
                    break;
                }
                url = this.url_queue.poll(500, TimeUnit.MILLISECONDS);
                if (url == null) {
                    continue;
                }
                if (this.stop_requested.get() || Thread.currentThread().isInterrupted()) {
                    break;
                }
                this.in_flight.incrementAndGet();
                counted_in_flight = true;
                RobotsService.RobotsCheck check = this.robots_service.check(url);
                if (!check.allowed) {
                    System.out.println(
                            "[Worker " + Thread.currentThread().getName() + "] disallowed by robots.txt: " + url);
                    continue;
                }
                long per_host_delay = (check.crawler_delay_ms > 0) ? check.crawler_delay_ms : 1000;
                this.rate_limiter.acquire(url, per_host_delay);
                if (this.stop_requested.get() || Thread.currentThread().isInterrupted()) {
                    break;
                }
                System.out.println("[Worker " + Thread.currentThread().getName()
                        + "] fetching: " + url);
                HttpFetcher.FetchResult res = this.http_fetcher.fetch_with_retries(url, 3);
                if (res == null) {
                    continue;
                }
                if (res.status_code < 200 || res.status_code >= 300) {
                    System.err.println("[Worker " + Thread.currentThread().getName()
                            + "] error for " + url + ": " + res.status_code);
                    continue;
                }
                if (!res.is_html()) {
                    continue;
                }
                this.storage_service.store_async(url, res.body);
                int stored = this.pages_stored.incrementAndGet();
                if (stored >= this.max_pages) {
                    this.stop_requested.set(true);
                    break;
                }
                Document doc = Jsoup.parse(res.body, url);
                Elements links = doc.select("a[href]");
                for (Element link : links) {
                    if (this.stop_requested.get() || Thread.currentThread().isInterrupted()) {
                        break;
                    }
                    String abs_url = link.attr("abs:href");
                    String normalized = UrlNormalizer.normalize(abs_url);
                    if (normalized != null && this.scope_policy.is_in_scope(normalized)) {
                        this.visited_manager.enqueue_if_new(normalized, url_queue);
                    }
                }
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException err) {
                System.err.println(
                        "[Worker " + Thread.currentThread().getName() + "] runtime exception: " + err);
                throw err;
            } catch (Exception err) {
                System.err.println("[Worker " + Thread.currentThread().getName()
                        + "] fatal exception: " + err);
                throw new RuntimeException(err);
            } finally {
                if (counted_in_flight) {
                    this.in_flight.decrementAndGet();
                }
            }
        }
        return null;
    }
}