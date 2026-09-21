package com.raft.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.OverlappingFileLockException;
import static org.junit.jupiter.api.Assertions.*;

class RaftPersistentStateTest {
    @TempDir Path directory;

    @Test void termAndVoteSurviveOrderlyReopen() throws Exception {
        try (var state = new RaftPersistentState(directory)) {
            assertEquals(0, state.currentTerm());
            assertNull(state.votedFor());
            state.save(7, "node2");
        }
        try (var state = new RaftPersistentState(directory)) {
            assertEquals(7, state.currentTerm());
            assertEquals("node2", state.votedFor());
            state.save(8, null);
        }
        try (var state = new RaftPersistentState(directory)) {
            assertEquals(8, state.currentTerm());
            assertNull(state.votedFor());
        }
    }

    @Test void rejectsTermRegressionAndChangingSameTermVote() throws Exception {
        try (var state = new RaftPersistentState(directory)) {
            state.save(3, "node1");
            assertThrows(IllegalArgumentException.class, () -> state.save(2, null));
            assertThrows(IllegalArgumentException.class, () -> state.save(3, "node2"));
            assertThrows(IllegalArgumentException.class, () -> state.save(3, null));
            state.save(3, "node1");
        }
    }

    @Test void preventsConcurrentStorageOwners() throws Exception {
        try (var state = new RaftPersistentState(directory)) {
            assertThrows(OverlappingFileLockException.class, () -> new RaftPersistentState(directory));
        }
        try (var reopened = new RaftPersistentState(directory)) {
            assertEquals(0, reopened.currentTerm());
        }
    }

    @Test void rejectsTruncatedHardStateRatherThanResettingTerm() throws Exception {
        Files.write(directory.resolve("hard-state.bin"), new byte[]{0, 0});
        assertThrows(java.io.IOException.class, () -> new RaftPersistentState(directory));
    }

    @Test void orphanedTemporaryFileDoesNotResetTerm() throws Exception {
        Files.write(directory.resolve("hard-state.tmp"), new byte[]{1, 2, 3});
        assertThrows(java.io.IOException.class, () -> new RaftPersistentState(directory));
        assertFalse(Files.exists(directory.resolve("hard-state.bin")));
    }

    @Test void leftoverTemporaryFileDoesNotOverridePersistedVote() throws Exception {
        try (var state = new RaftPersistentState(directory)) { state.save(9, "node2"); }
        Files.write(directory.resolve("hard-state.tmp"), new byte[]{1, 2, 3});
        try (var state = new RaftPersistentState(directory)) {
            assertEquals(9, state.currentTerm());
            assertEquals("node2", state.votedFor());
            assertThrows(IllegalArgumentException.class, () -> state.save(9, "node3"));
        }
    }

    @Test void missingPreviouslyPersistedHardStateFailsClosedInsteadOfForgettingVote() throws Exception {
        try (var state = new RaftPersistentState(directory)) { state.save(42, "node2"); }
        Files.delete(directory.resolve("hard-state.bin"));
        java.io.IOException failure = assertThrows(java.io.IOException.class,
                () -> new RaftPersistentState(directory));
        assertTrue(failure.getMessage().contains("refusing term reset"));
        assertFalse(Files.exists(directory.resolve("hard-state.bin")));
    }
}
