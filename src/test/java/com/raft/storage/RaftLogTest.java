package com.raft.storage;

import com.raft.rpc.LogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {
    @TempDir Path root;
    private static LogEntry entry(long index, long term) { return LogEntry.newBuilder().setIndex(index).setTerm(term).build(); }

    @Test void rejectsGapsAndImplicitOverwrite() throws Exception {
        RaftLog log = new RaftLog(root.toString(), "node1");
        try {
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(2, 1)));
            log.append(entry(1, 2));
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(1, 3)));
            assertThrows(IllegalArgumentException.class, () -> log.append(entry(2, 1)));
            assertEquals(1, log.getLastIndex());
            assertEquals(2, log.getLastTerm());
        } finally { log.close(); }
    }

    @Test void explicitSuffixTruncationAndBoundedRangesMaintainMetadataOnReopen() throws Exception {
        RaftLog log = new RaftLog(root.toString(), "node1");
        try {
            log.append(entry(1, 1)); log.append(entry(2, 2)); log.append(entry(3, 2));
            log.truncateAt(2); log.append(entry(2, 3));
            assertEquals(java.util.List.of(entry(1, 1)), log.getEntriesFrom(1, 1));
            assertTrue(log.getEntriesFrom(3, 64).isEmpty());
            assertEquals(0, log.getTerm(0));
        } finally { log.close(); }
        log = new RaftLog(root.toString(), "node1");
        try { assertEquals(2, log.getLastIndex()); assertEquals(3, log.getLastTerm()); }
        finally { log.close(); }
    }
}
