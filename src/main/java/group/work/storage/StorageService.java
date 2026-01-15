package group.work.storage;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class StorageService {
    private final String output_path;
    private BlockingQueue<PageRecord> write_queue = new LinkedBlockingQueue<>();
    private Thread writer_thread;

    public StorageService(String output_path) {
        this.output_path = output_path;
    }

    public void store_async(String url, String content) {
        long ts = System.currentTimeMillis();
        this.write_queue.offer(new PageRecord(url, ts, content));
    }

    private void writer_loop() {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(this.output_path, true)))) {
            while (true) {
                PageRecord r = this.write_queue.take();
                if (r == PageRecord.POISON) {
                    break;
                }
                out.println("############## " + r.url + " # " + r.timestamp_ms + " #########");
                out.println(r.content);
                out.println();
                out.flush();
            }
        } catch (IOException err) {
            System.err.println("[StorageService] failed to write to storage: " + err.getMessage());
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
        }
    }

    public void start() {
        this.writer_thread = new Thread(this::writer_loop, "storage-writer");
        this.writer_thread.setDaemon(true);
        this.writer_thread.start();
    }

    public void stop() {
        try {
            this.write_queue.offer(PageRecord.POISON);
            if (this.writer_thread != null) {
                this.writer_thread.join(3000);
            }
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
        }
    }
}
