package com.raft.storage;

import com.raft.rpc.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Length-prefixed protobuf WAL. Complete records are forced before publishing metadata.
 * Torn EOF tails are repairable; complete corruption fails closed. No checksum is present.
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class);
    // Above the 32 KiB client-command limit; bounds allocation even for corrupt prefixes.
    public static final int MAX_RECORD_BYTES = 1024 * 1024;
    private final File dataFile;
    private final FileChannel channel;
    private final List<LogEntry> inMemoryLogs = new ArrayList<>();
    // Actual wire offsets, not sizes inferred by re-serializing recovered protobufs.
    private final List<Long> recordOffsets = new ArrayList<>();
    private final AtomicLong lastIndex = new AtomicLong(0);
    private final AtomicLong lastTerm = new AtomicLong(0);

    public RaftLog(String dataDir, String nodeId) throws IOException {
        Path dirPath = Path.of(dataDir);
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
        }
        this.dataFile = new File(dataDir, "raft_log_" + nodeId + ".bin");
        boolean isNew = !dataFile.exists();
        this.channel = FileChannel.open(dataFile.toPath(), 
            StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        
        try {
            if (!isNew) recover();
            else {
                channel.force(true);
                StorageDurability.forceDirectory(dirPath);
            }
        } catch (IOException | RuntimeException e) { channel.close(); throw e; }
    }

    /**
     * Appends an entry to the log and flushes to disk.
     */
    public synchronized void append(LogEntry entry) throws IOException {
        // Conflict reconciliation is explicit in RaftNode, never an implicit overwrite.
        if (entry.getIndex() != lastIndex.get() + 1 || entry.getIndex() <= 0
                || entry.getTerm() < lastTerm.get())
            throw new IllegalArgumentException("Log append must be contiguous with nondecreasing terms");

        int length = entry.getSerializedSize();
        if (length < 1 || length > MAX_RECORD_BYTES)
            throw new IllegalArgumentException("WAL record length outside 1.." + MAX_RECORD_BYTES);
        byte[] bytes = entry.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        long recordStart = channel.position();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        channel.force(true); // fsync

        inMemoryLogs.add(entry);
        recordOffsets.add(recordStart);
        lastIndex.set(entry.getIndex());
        lastTerm.set(entry.getTerm());
    }

    public synchronized void appendAll(List<LogEntry> entries) throws IOException {
        for (LogEntry entry : entries) {
            append(entry);
        }
    }

    private void recover() throws IOException {
        channel.position(0);
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        while (true) {
            long recordStart = channel.position();
            sizeBuffer.clear();
            int headerBytes = readFully(channel, sizeBuffer);
            if (headerBytes == 0) break;
            if (headerBytes < 4) {
                discardIncompleteTail(recordStart);
                break;
            }
            sizeBuffer.flip();
            int size = sizeBuffer.getInt();
            if (size < 1 || size > MAX_RECORD_BYTES)
                throw new IOException("Invalid WAL record length " + size + " at byte " + recordStart);
            if (size > channel.size() - channel.position()) {
                discardIncompleteTail(recordStart);
                break;
            }
            ByteBuffer dataBuffer = ByteBuffer.allocate(size);
            if (readFully(channel, dataBuffer) != size) {
                discardIncompleteTail(recordStart);
                break;
            }
            LogEntry entry = LogEntry.parseFrom(dataBuffer.array());
            if (entry.getIndex() != lastIndex.get() + 1 || entry.getTerm() < lastTerm.get())
                throw new IOException("Invalid WAL index/term sequence");
            inMemoryLogs.add(entry);
            recordOffsets.add(recordStart);
            lastIndex.set(entry.getIndex());
            lastTerm.set(entry.getTerm());
        }
        channel.position(channel.size());
        logger.info("Recovered {} entries from log. Last index: {}, Last term: {}", 
            inMemoryLogs.size(), lastIndex.get(), lastTerm.get());
    }

    /** Reads through short reads until full or EOF. Kept package-visible to test short reads. */
    static int readFully(ReadableByteChannel source, ByteBuffer target) throws IOException {
        int start = target.position();
        while (target.hasRemaining()) {
            if (source.read(target) == -1) break;
        }
        return target.position() - start;
    }

    private void discardIncompleteTail(long validBytes) throws IOException {
        long discarded = channel.size() - validBytes;
        channel.truncate(validBytes);
        channel.position(validBytes);
        channel.force(true);
        logger.warn("Discarded {} incomplete final WAL bytes at offset {}; retained {} entries",
                discarded, validBytes, inMemoryLogs.size());
    }

    public synchronized void truncateAt(long startIndex) throws IOException {
        if (startIndex < 1 || startIndex > lastIndex.get() + 1)
            throw new IllegalArgumentException("Invalid truncation index");
        if (startIndex == lastIndex.get() + 1) return;
        
        int count = Math.toIntExact(startIndex - 1);
        long offset = recordOffsets.get(count);

        channel.truncate(offset);
        channel.position(offset);
        channel.force(true);
        
        while (inMemoryLogs.size() > count) {
            inMemoryLogs.remove(inMemoryLogs.size() - 1);
            recordOffsets.remove(recordOffsets.size() - 1);
        }
        
        if (inMemoryLogs.isEmpty()) {
            lastIndex.set(0);
            lastTerm.set(0);
        } else {
            LogEntry last = inMemoryLogs.get(inMemoryLogs.size() - 1);
            lastIndex.set(last.getIndex());
            lastTerm.set(last.getTerm());
        }
    }

    public synchronized LogEntry getEntry(long index) {
        if (index <= 0 || index > lastIndex.get()) return null;
        return inMemoryLogs.get((int) index - 1);
    }

    public synchronized List<LogEntry> getEntriesSince(long index) {
        if (index < 0) throw new IllegalArgumentException("Negative log index");
        if (index > lastIndex.get()) return List.of();
        return new ArrayList<>(inMemoryLogs.subList((int) index, inMemoryLogs.size()));
    }

    /** Inclusive first index; a bounded immutable snapshot for one AppendEntries. */
    public synchronized List<LogEntry> getEntriesFrom(long firstIndex, int limit) {
        if (firstIndex < 1 || firstIndex > lastIndex.get() + 1 || limit < 1)
            throw new IllegalArgumentException("Invalid log range");
        int from = Math.toIntExact(firstIndex - 1);
        int to = (int) Math.min(inMemoryLogs.size(), (long) from + limit);
        return List.copyOf(inMemoryLogs.subList(from, to));
    }

    public synchronized long getTerm(long index) {
        if (index == 0) return 0;
        LogEntry entry = getEntry(index);
        if (entry == null) throw new IllegalArgumentException("Missing log index " + index);
        return entry.getTerm();
    }

    public long getLastIndex() {
        return lastIndex.get();
    }

    public long getLastTerm() {
        return lastTerm.get();
    }

    public synchronized void close() throws IOException {
        channel.close();
    }
}
