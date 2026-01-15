package group.work.storage;

public class PageRecord {
    public final String url;
    public final long timestamp_ms;
    public final String content;

    public PageRecord(String url, long timestamp_ms, String content) {
        this.url = url;
        this.timestamp_ms = timestamp_ms;
        this.content = content;
    }

    // Special record to signal the writer thread to stop
    public static final PageRecord POISON = new PageRecord("__POISON__", -1, "");
}
