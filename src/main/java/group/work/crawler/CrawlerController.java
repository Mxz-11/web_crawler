package group.work.crawler;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import group.work.handlers.HttpFetcher;
import group.work.handlers.RobotsService;
import group.work.handlers.ScopePolicy;
import group.work.helpers.RateLimiter;
import group.work.helpers.UrlNormalizer;
import group.work.helpers.VisitedManager;
import group.work.storage.StorageService;

// We run x crawler workers in a fixed thread pool
// Each worker is submitted as a Callable, so we can track failures via Future.get();
public class CrawlerController {
    private final BlockingQueue<String> url_queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor_service;
    private final ScheduledExecutorService monitor_service = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "crawler-monitor");
        t.setDaemon(true);
        return t;
    });
    private final VisitedManager visited_manager = new VisitedManager();
    private final StorageService storage_service = new StorageService("crawled_data.txt");
    private final RobotsService robots_service = new RobotsService();
    private final RateLimiter rate_limiter = new RateLimiter(1000);
    private final HttpFetcher http_fetcher = new HttpFetcher("Crawler/1.0");
    private final ScopePolicy scope_policy;
    private final int max_pages;
    private final int num_threads;
    private final AtomicInteger pages_stored = new AtomicInteger(0);
    private final AtomicInteger in_flight = new AtomicInteger(0);
    private final AtomicBoolean stop_req = new AtomicBoolean(false);
    private final AtomicBoolean shutdown_started = new AtomicBoolean(false);
    private final List<Future<Void>> worker_futures = new CopyOnWriteArrayList<>();

    public CrawlerController(List<String> seeds, int max_pages, int num_threads) {
        this.max_pages = max_pages;
        this.num_threads = num_threads;
        Set<String> allowed_hosts = new HashSet<>();
        for (String seed : seeds) {
            String normalized = UrlNormalizer.normalize(seed);
            if (normalized != null) {
                try {
                    String host = new URI(normalized).getHost();
                    if (host != null) {
                        allowed_hosts.add(host.toLowerCase());
                    }
                } catch (Exception err) {
                }
            }
        }
        this.scope_policy = new ScopePolicy(allowed_hosts);
        this.executor_service = Executors.newFixedThreadPool(num_threads, new ThreadFactory() {
            private final ThreadFactory default_fac = Executors.defaultThreadFactory();
            private final AtomicInteger idx = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = default_fac.newThread(r);
                t.setName("crawler-worker-" + idx.getAndIncrement());
                t.setUncaughtExceptionHandler(
                        (th, ex) -> System.err
                                .println("[Crawler-worker] uncaught exception in " + th.getName() + ": " + ex));
                return t;
            }
        });
        for (String seed : seeds) {
            String normalized = UrlNormalizer.normalize(seed);
            if (normalized != null) {
                this.visited_manager.enqueue_if_new(normalized, url_queue);
            }
        }
    }

    // Stop conditions:
    // Hard limit: stop once we have stored max_pages
    // Completion: stop when the queue stays empty
    // If a worker crashes, we log the root cause and optionally respawn a worker
    public void start() {
        this.storage_service.start();
        for (int i = 0; i < this.num_threads; i++) {
            spawn_worker();
        }
        final AtomicInteger empty_stable_ticks = new AtomicInteger(0);
        this.monitor_service.scheduleAtFixedRate(() -> {
            System.out.println("[Controller] stop = " + this.stop_req.get()
                    + " stored = " + this.pages_stored.get()
                    + " queue = " + this.url_queue.size()
                    + " inFlight = " + this.in_flight.get()
                    + " futures = " + this.worker_futures.size());
            if (this.stop_req.get()) {
                shutdown();
                return;
            }
            if (this.pages_stored.get() >= this.max_pages) {
                System.out.println("[Controller] reached max_pages (" + this.max_pages + "), stopping");
                shutdown();
                return;
            }
            if (!this.stop_req.get()) {
                if (this.pages_stored.get() >= this.max_pages) {
                    System.out.println("[Controller] reached max_pages (" + this.max_pages + "), stopping");
                    shutdown();
                    return;
                }
                if (this.url_queue.isEmpty() && this.in_flight.get() == 0) {
                    int ticks = empty_stable_ticks.incrementAndGet();
                    if (ticks >= 6) {
                        System.out.println("[Controller] queue empty and no in-flight work, stopping");
                        shutdown();
                        return;
                    }
                } else {
                    empty_stable_ticks.set(0);
                }
            }
            for (Iterator<Future<Void>> it = this.worker_futures.iterator(); it.hasNext();) {
                Future<Void> f = it.next();
                if (!f.isDone())
                    continue;
                try {
                    f.get();
                } catch (CancellationException err) {
                } catch (ExecutionException err) {
                    System.err.println("[Controller] worker crashed with exception: " + err.getCause());
                    if (!this.stop_req.get()) {
                        System.err.println("[Controller] respawning a new worker to maintain concurrency");
                        this.spawn_worker();
                    }
                } catch (InterruptedException err) {
                    Thread.currentThread().interrupt();
                } finally {
                    this.worker_futures.remove(f);
                }
            }
        }, 500, 500, TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    private void spawn_worker() {
        WorkerTask task = new WorkerTask(
                this.url_queue,
                this.visited_manager,
                this.rate_limiter,
                this.storage_service,
                this.robots_service,
                this.http_fetcher,
                this.scope_policy,
                this.pages_stored,
                this.in_flight,
                this.stop_req,
                this.max_pages);
        Future<Void> future = this.executor_service.submit(task);
        this.worker_futures.add(future);
    }

    public void shutdown() {
        if (!this.shutdown_started.compareAndSet(false, true)) {
            return;
        }
        this.stop_req.set(true);
        System.out.println("[Controller] shutting down crawler");
        this.monitor_service.shutdownNow();
        for (Future<Void> f : new ArrayList<>(this.worker_futures)) {
            f.cancel(true);
        }
        this.executor_service.shutdownNow();
        try {
            this.executor_service.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.storage_service.stop();
        System.out.println("[Controller] stopped; pages stored=" + this.pages_stored.get()
                + ", seen URLs=" + this.visited_manager.get_seen_count());
    }
}