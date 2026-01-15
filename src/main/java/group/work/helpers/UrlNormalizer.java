package group.work.helpers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

public class UrlNormalizer {
    public static String normalize(String url) {
        if (url == null) {
            return null;
        }
        url = url.trim();
        if (url.isEmpty()) {
            return null;
        }
        String lower = url.toLowerCase(Locale.ROOT);
        if (lower.startsWith("mailto:") || lower.startsWith("javascript:") || lower.startsWith("tel:")) {
            return null;
        }
        try {
            URI u = new URI(url);
            String scheme = u.getScheme();
            if (scheme == null) {
                return null;
            }
            scheme = scheme.toLowerCase(Locale.ROOT);
            if (!scheme.equals("http") && !scheme.equals("https")) {
                return null;
            }
            String host = u.getHost();
            if (host == null) {
                return null;
            }
            host = host.toLowerCase();
            int port = u.getPort();
            if ((scheme.equals("http") && port == 80) || (scheme.equals("https") && port == 443)) {
                port = -1;
            }
            String path = u.getRawPath();
            if (path == null || path.isEmpty()) {
                path = "/";
            }
            String query = u.getRawQuery();
            URI normalized = new URI(scheme, u.getRawUserInfo(), host, port, path, query, null);
            return normalized.toString();
        } catch (URISyntaxException err) {
            return null;
        }
    }
}
