package group.work.handlers;

import java.net.URI;
import java.util.Locale;
import java.util.Set;

public class ScopePolicy {
    private final Set<String> allowed_hosts;

    public ScopePolicy(Set<String> allowed_hosts) {
        this.allowed_hosts = allowed_hosts;
    }

    // We restrict discovered links to a whitelist of allowed hosts
    // Without this limitation, the crawler would cause unbouded queue growth or
    // unpredictable runtimes
    public boolean is_in_scope(String url) {
        try {
            String host = new URI(url).getHost();
            if (host == null) {
                return false;
            }
            host = host.toLowerCase(Locale.ROOT);
            for (String allowed : this.allowed_hosts) {
                if (host.equals(allowed) || host.endsWith("." + allowed)) {
                    return true;
                }
            }
            return false;
        } catch (Exception err) {
            return false;
        }
    }
}
