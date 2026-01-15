package group.work.handlers;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;

public class HttpFetcher {
    public static class FetchResult {
        public final int status_code;
        public final String body;
        public final String content_type;
        public final Long retry_after_ms;

        public FetchResult(int status_code, String body, String content_type, Long retry_after_ms) {
            this.status_code = status_code;
            this.body = body;
            this.content_type = content_type;
            this.retry_after_ms = retry_after_ms;
        }

        public boolean is_html() {
            if (this.content_type == null) {
                return false;
            }
            return this.content_type.toLowerCase(Locale.ROOT).contains("text/html");
        }
    }

    private final HttpClient client;
    private final String user_agent;
    private final Random random = new Random();

    public HttpFetcher(String user_agent) {
        this.user_agent = user_agent;
        this.client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    // We retry failures like HTTP 429, HTTP 5xx and network or timeouts problems
    // We use a backoff with jitter to avoid synchronized retry storms
    // If Retry-After is provided, we respect it
    public FetchResult fetch_with_retries(String url, int maxAttempts) {
        long backoff = 250;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                FetchResult res = this.fetch_once(url);
                if (res == null)
                    return null;
                if (res.status_code >= 200 && res.status_code < 300) {
                    return res;
                }
                if (res.status_code == 429 || (res.status_code >= 500 && res.status_code <= 599)) {
                    long wait = this.compute_wait_ms(res, backoff);
                    this.sleep_quiet(wait);
                    backoff = Math.min(backoff * 2, 2000);
                    continue;
                }
                return res;
            } catch (InterruptedException err) {
                Thread.currentThread().interrupt();
                return null;
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    return null;
                }
                long jitter = this.random.nextInt(120);
                try {
                    this.sleep_quiet(backoff + jitter);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                backoff = Math.min(backoff * 2, 2000);
            }
        }
        return null;
    }

    private FetchResult fetch_once(String url) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(8))
                .header("User-Agent", this.user_agent)
                .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                .GET()
                .build();
        HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        String ct = res.headers().firstValue("content-type").orElse(null);
        Long retryAfterMs = this.parse_retry_after(res.headers().firstValue("retry-after"));
        return new FetchResult(res.statusCode(), res.body(), ct, retryAfterMs);
    }

    private long compute_wait_ms(FetchResult res, long backoff) {
        if (res.retry_after_ms != null && res.retry_after_ms > 0) {
            return Math.min(res.retry_after_ms, 10000);
        }
        long jitter = this.random.nextInt(120);
        return backoff + jitter;
    }

    private Long parse_retry_after(Optional<String> header) {
        if (header == null || header.isEmpty()) {
            return null;
        }
        String v = header.get().trim();
        try {
            long seconds = Long.parseLong(v);
            return seconds * 1000L;
        } catch (NumberFormatException err) {
            return null;
        }
    }

    private void sleep_quiet(long ms) throws InterruptedException {
        if (ms <= 0) {
            return;
        }
        Thread.sleep(ms);
    }
}
