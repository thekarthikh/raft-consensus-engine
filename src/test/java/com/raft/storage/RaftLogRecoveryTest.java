package com.raft.storage;

import com.google.protobuf.ByteString;
import com.raft.rpc.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogRecoveryTest {
    @TempDir Path root;

    private LogEntry entry(long index, long term) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term)
                .setData(ByteString.copyFromUtf8("command-" + index)).build();
    }

    private Path wal() { return root.resolve("raft_log_node1.bin"); }

    private long seed() throws Exception {
        RaftLog log = new RaftLog(root.toString(), "node1");
        try { log.append(entry(1, 2)); log.append(entry(2, 3)); }
        finally { log.close(); }
        return Files.size(wal());
    }

    private void assertPrefixAndResume(long validBytes) throws Exception {
        RaftLog log = new RaftLog(root.toString(), "node1");
        try {
            assertEquals(List.of(entry(1, 2), entry(2, 3)), log.getEntriesFrom(1, 64));
            assertEquals(2, log.getLastIndex());
            assertEquals(3, log.getLastTerm());
            assertEquals(validBytes, Files.size(wal()));
            log.append(entry(3, 4));
        } finally { log.close(); }
        log = new RaftLog(root.toString(), "node1");
        try {
            assertEquals(List.of(entry(1, 2), entry(2, 3), entry(3, 4)), log.getEntriesSince(0));
            assertEquals(3, log.getLastIndex());
            assertEquals(4, log.getLastTerm());
        } finally { log.close(); }
    }

    // Byte fixtures simulate interrupted writes; they are not mislabeled as process crashes.
    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void partialFinalHeaderPreservesCompletePrefix(int bytes) throws Exception {
        long validBytes = seed();
        byte[] header = ByteBuffer.allocate(4).putInt(30).array();
        Files.write(wal(), java.util.Arrays.copyOf(header, bytes), StandardOpenOption.APPEND);
        assertPrefixAndResume(validBytes);
    }

    @ParameterizedTest @ValueSource(ints = {0, 1, 6})
    void partialFinalPayloadPreservesCompletePrefix(int bytes) throws Exception {
        long validBytes = seed();
        byte[] payload = entry(3, 4).toByteArray();
        assertTrue(bytes < payload.length);
        ByteBuffer tail = ByteBuffer.allocate(4 + bytes).putInt(payload.length).put(payload, 0, bytes);
        Files.write(wal(), tail.array(), StandardOpenOption.APPEND);
        assertPrefixAndResume(validBytes);
    }

    @ParameterizedTest @ValueSource(ints = {0, -1, 1048577, 2147483647})
    void invalidLengthsFailClosedWithoutDeletingValidPrefix(int length) throws Exception {
        long validBytes = seed();
        byte[] prefix = Files.readAllBytes(wal());
        Files.write(wal(), ByteBuffer.allocate(4).putInt(length).array(), StandardOpenOption.APPEND);
        IOException failure = assertThrows(IOException.class, () -> new RaftLog(root.toString(), "node1"));
        assertTrue(failure.getMessage().contains("Invalid WAL record length"));
        assertEquals(validBytes + 4, Files.size(wal()));
        assertArrayEquals(prefix, java.util.Arrays.copyOf(Files.readAllBytes(wal()), prefix.length));
    }

    @Test void emptyWalAndOnlyPartialHeaderRecoverEmptyLog() throws Exception {
        Files.write(wal(), new byte[0]);
        RaftLog log = new RaftLog(root.toString(), "node1");
        try { assertEquals(0, log.getLastIndex()); assertEquals(0, log.getLastTerm()); }
        finally { log.close(); }
        Files.write(wal(), new byte[]{0, 0, 0});
        log = new RaftLog(root.toString(), "node1");
        try { assertEquals(0, log.getLastIndex()); assertEquals(0, Files.size(wal())); }
        finally { log.close(); }
    }

    @Test void completeCorruptRecordIsNotTreatedAsTornTail() throws Exception {
        seed();
        Files.write(wal(), new byte[]{0, 0, 0, 1, (byte) 0xff}, StandardOpenOption.APPEND);
        byte[] original = Files.readAllBytes(wal());
        assertThrows(IOException.class, () -> new RaftLog(root.toString(), "node1"));
        assertArrayEquals(original, Files.readAllBytes(wal()));
    }

    @Test void completeIndexGapFailsClosed() throws Exception {
        seed();
        byte[] payload = entry(4, 4).toByteArray();
        Files.write(wal(), ByteBuffer.allocate(4 + payload.length).putInt(payload.length).put(payload).array(),
                StandardOpenOption.APPEND);
        byte[] original = Files.readAllBytes(wal());
        assertThrows(IOException.class, () -> new RaftLog(root.toString(), "node1"));
        assertArrayEquals(original, Files.readAllBytes(wal()));
    }

    @Test void oversizedAppendDoesNotWriteAnything() throws Exception {
        seed();
        RaftLog log = new RaftLog(root.toString(), "node1");
        try {
            long size = Files.size(wal());
            LogEntry huge = entry(3, 3).toBuilder()
                    .setData(ByteString.copyFrom(new byte[RaftLog.MAX_RECORD_BYTES])).build();
            assertThrows(IllegalArgumentException.class, () -> log.append(huge));
            assertEquals(size, Files.size(wal()));
            assertEquals(2, log.getLastIndex());
        } finally { log.close(); }
    }

    @Test void shortReadsAreAccumulatedForHeaderAndPayloadAndStopAtEof() throws Exception {
        byte[] bytes = new byte[]{0, 0, 0, 6, 1, 2, 3, 4, 5, 6};
        try (ReadableByteChannel source = new ReadableByteChannel() {
            int position;
            boolean open = true;
            public int read(ByteBuffer target) {
                if (position == bytes.length) return -1;
                target.put(bytes[position++]); // deliberately return just one byte per read
                return 1;
            }
            public boolean isOpen() { return open; }
            public void close() { open = false; }
        }) {
            ByteBuffer header = ByteBuffer.allocate(4);
            assertEquals(4, RaftLog.readFully(source, header));
            assertEquals(6, header.flip().getInt());
            ByteBuffer payload = ByteBuffer.allocate(8);
            assertEquals(6, RaftLog.readFully(source, payload));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 0, 0}, payload.array());
        }
    }

    @Test void truncationAtBeginningAndRangeBoundariesRemainCorrectOnRestart() throws Exception {
        seed();
        RaftLog log = new RaftLog(root.toString(), "node1");
        try {
            assertThrows(IllegalArgumentException.class, () -> log.truncateAt(0));
            assertThrows(IllegalArgumentException.class, () -> log.truncateAt(4));
            assertThrows(IllegalArgumentException.class, () -> log.getEntriesFrom(0, 1));
            assertThrows(IllegalArgumentException.class, () -> log.getEntriesFrom(1, 0));
            assertEquals(List.of(entry(2, 3)), log.getEntriesFrom(2, Integer.MAX_VALUE));
            assertTrue(log.getEntriesFrom(3, 1).isEmpty());
            log.truncateAt(3); // no-op
            assertEquals(2, log.getLastIndex());
            log.truncateAt(1);
            assertEquals(0, log.getLastIndex()); assertEquals(0, log.getLastTerm());
            assertEquals(0, Files.size(wal()));
            log.append(entry(1, 1));
        } finally { log.close(); }
        RaftLog reopened = new RaftLog(root.toString(), "node1");
        try { assertEquals(List.of(entry(1, 1)), reopened.getEntriesFrom(1, 64)); }
        finally { reopened.close(); }
    }

    @Test void suffixTruncationUsesOriginalRecoveredRecordOffsets() throws Exception {
        byte[] canonical = entry(1, 2).toByteArray();
        // Protobuf permits repeated occurrences of a singular field (last value wins).
        // Parsing/re-serializing this valid record produces fewer bytes than were stored.
        byte[] payload = java.util.Arrays.copyOf(canonical, canonical.length + 2);
        payload[canonical.length] = 8; payload[canonical.length + 1] = 2; // duplicate term=2
        Files.write(wal(), ByteBuffer.allocate(4 + payload.length).putInt(payload.length).put(payload).array());
        RaftLog log = new RaftLog(root.toString(), "node1");
        try {
            assertEquals(entry(1, 2), log.getEntry(1));
            log.append(entry(2, 3)); log.append(entry(3, 3));
            log.truncateAt(2);
            assertEquals(4 + payload.length, Files.size(wal()), "retain complete original wire record");
            log.append(entry(2, 4));
        } finally { log.close(); }
        RaftLog reopened = new RaftLog(root.toString(), "node1");
        try {
            assertEquals(List.of(entry(1, 2), entry(2, 4)), reopened.getEntriesFrom(1, 64));
            assertEquals(2, reopened.getLastIndex()); assertEquals(4, reopened.getLastTerm());
        } finally { reopened.close(); }
    }
}
