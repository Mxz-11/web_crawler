package group.work.helpers;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class VisitedManager {
    private final Set<String> seen = ConcurrentHashMap.newKeySet();

    public boolean enqueue_if_new(String url, BlockingQueue<String> queue) {
        if (url == null) {
            return false;
        }
        if (seen.add(url)) {
            queue.offer(url);
            return true;
        }
        return false;
    }

    public int get_seen_count() {
        return seen.size();
    }
}
