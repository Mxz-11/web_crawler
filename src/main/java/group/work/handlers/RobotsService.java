package group.work.handlers;

import java.io.BufferedReader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RobotsService {
    public static class RobotsCheck {
        public final boolean allowed;
        public final long crawler_delay_ms;

        public RobotsCheck(boolean allowed, long crawler_delay_ms) {
            this.allowed = allowed;
            this.crawler_delay_ms = crawler_delay_ms;
        }
    }

    private static class Group {
        final List<String> allow = new ArrayList<>();
        final List<String> disallow = new ArrayList<>();
        long crawl_delay_s = -1;

        private int best_prefix_match_len(List<String> rules, String path) {
            int best = -1;
            for (String r : rules) {
                if (r == null || r.isEmpty()) {
                    continue;
                }
                if (path.startsWith(r)) {
                    best = Math.max(best, r.length());
                }
            }
            return best;
        }

        boolean is_allowed(String path) {
            int best_allow = best_prefix_match_len(allow, path);
            int best_disallow = best_prefix_match_len(disallow, path);
            return best_allow >= best_disallow;
        }
    }

    private static class RobotsRules {
        final long fetched_at_ms;
        final long ttl_ms;
        final Group group;

        RobotsRules(long fetched_at_ms, long ttl_ms, Group group) {
            this.fetched_at_ms = fetched_at_ms;
            this.ttl_ms = ttl_ms;
            this.group = group;
        }

        boolean is_expired() {
            return System.currentTimeMillis() - this.fetched_at_ms > this.ttl_ms;
        }
    }

    private final HttpClient client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private final ConcurrentHashMap<String, RobotsRules> cache = new ConcurrentHashMap<>();
    private final String user_agent = "Crawler";
    private final long cache_ttl_ms = 6L * 60L * 60L * 1000L; // 6 [h]

    public RobotsCheck check(String url) {
        String host = host(url);
        if (host == null)
            return new RobotsCheck(true, 0);

        RobotsRules rules = cache.get(host);
        if (rules == null || rules.is_expired()) {
            rules = this.fetch_and_parse(host);
            if (rules != null)
                cache.put(host, rules);
        }
        if (rules == null || rules.group == null) {
            return new RobotsCheck(true, 0);
        }
        String path = path(url);
        boolean allowed = rules.group.is_allowed(path);
        long delay_ms = rules.group.crawl_delay_s > 0 ? rules.group.crawl_delay_s * 1000L : 0;
        return new RobotsCheck(allowed, delay_ms);
    }

    // In this way, it supports User-agent, Allow, Disallow and Crawl-delay
    // It chooses the most appropiate group for the crawler
    // Uses longest match wins login between Allow and Disallow
    private Group parse_robots(String body) {
        Map<String, Group> groups = new LinkedHashMap<>();
        String current_agent = null;
        try (BufferedReader br = new BufferedReader(new StringReader(body))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = strip_comment(line).trim();
                if (line.isEmpty()) {
                    continue;
                }
                int idx = line.indexOf(':');
                if (idx < 0) {
                    continue;
                }
                String key = line.substring(0, idx).trim().toLowerCase(Locale.ROOT);
                String val = line.substring(idx + 1).trim();
                if ("user-agent".equals(key)) {
                    current_agent = val.toLowerCase(Locale.ROOT);
                    groups.computeIfAbsent(current_agent, k -> new Group());
                    continue;
                }
                if (current_agent == null) {
                    continue;
                }
                Group g = groups.computeIfAbsent(current_agent, k -> new Group());
                switch (key) {
                    case "allow" -> g.allow.add(val);
                    case "disallow" -> {
                        if (!val.isEmpty()) {
                            g.disallow.add(val);
                        }
                    }
                    case "crawl-delay" -> {
                        try {
                            g.crawl_delay_s = Long.parseLong(val);
                        } catch (NumberFormatException err) {
                        }
                    }
                }
            }
        } catch (Exception err) {
        }
        String user_agent = this.user_agent.toLowerCase(Locale.ROOT);
        if (groups.containsKey(user_agent)) {
            return groups.get(user_agent);
        }
        if (groups.containsKey("*")) {
            return groups.get("*");
        }
        return new Group();
    }

    private RobotsRules try_fetch(String robots_url) {
        try {
            HttpRequest req = HttpRequest.newBuilder().uri(URI.create(robots_url)).timeout(Duration.ofSeconds(5))
                    .header("User-Agent", this.user_agent).GET().build();
            HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() >= 400) {
                return null;
            }
            Group g = this.parse_robots(res.body());
            return new RobotsRules(System.currentTimeMillis(), cache_ttl_ms, g);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception err) {
            return null;
        }
    }

    private RobotsRules fetch_and_parse(String host) {
        RobotsRules https = this.try_fetch("https://" + host + "/robots.txt");
        if (https != null)
            return https;
        RobotsRules http = this.try_fetch("http://" + host + "/robots.txt");
        return http;
    }

    private String host(String url) {
        try {
            return new URI(url).getHost();
        } catch (URISyntaxException err) {
            return null;
        }
    }

    private String path(String url) {
        try {
            URI u = new URI(url);
            String p = u.getRawPath();
            if (p == null || p.isEmpty()) {
                return "/";
            }
            return p;
        } catch (URISyntaxException err) {
            return null;
        }
    }

    private String strip_comment(String line) {
        int idx = line.indexOf('#');
        return (idx >= 0) ? line.substring(0, idx) : line;
    }
}
