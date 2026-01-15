package group.work.helpers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RateLimiter {
    private final ConcurrentHashMap<String, AtomicLong> next_allowed = new ConcurrentHashMap<>();
    private final long default_delay_ms;

    public RateLimiter(long default_delay_ms) {
        this.default_delay_ms = default_delay_ms;
    }

    public void acquire(String url) {
        this.acquire(url, this.default_delay_ms);
    }

    public void acquire(String url, long delay_ms) {
        String host = this.get_host(url);
        if (host == null) {
            return;
        }
        AtomicLong slot = this.next_allowed.computeIfAbsent(host, h -> new AtomicLong(0));
        long now = System.currentTimeMillis();
        long old = slot.getAndUpdate(prev -> Math.max(prev, now) + delay_ms);
        long allowed_t = Math.max(old, now);
        long sleep = allowed_t - now;
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private String get_host(String url) {
        try {
            return new URI(url).getHost();
        } catch (URISyntaxException err) {
            return null;
        }
    }
}
