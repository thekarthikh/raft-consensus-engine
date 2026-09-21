package com.raft.core;

import com.raft.rpc.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RaftAppendEntriesTest {
    @TempDir Path root;
    private RaftNode node() throws Exception {
        return new RaftNode("node2", 0, List.of("node1", "node2", "node3"), root, 1500, 3000, 500);
    }
    private static LogEntry entry(long index, long term, String value) {
        return RaftReplicationIntegrationTest.entry(index, term, value);
    }
    private static AppendEntriesRequest request(long term, long previous, long previousTerm, long commit, LogEntry... entries) {
        return AppendEntriesRequest.newBuilder().setLeaderId("node1").setTerm(term).setPrevLogIndex(previous)
                .setPrevLogTerm(previousTerm).setLeaderCommit(commit).addAllEntries(List.of(entries)).build();
    }
    private static Capture send(RaftNode node, AppendEntriesRequest request) {
        Capture capture = new Capture(); node.appendEntries(request, capture); return capture;
    }

    @Test void missingOrWrongPreviousLogRejectsWithoutAppending() throws Exception {
        RaftNode node = node();
        try {
            assertTrue(send(node, request(2, 0, 0, 0, entry(1, 1, "a"))).response.getSuccess());
            assertFalse(send(node, request(2, 2, 1, 0, entry(3, 2, "b"))).response.getSuccess());
            assertFalse(send(node, request(2, 1, 2, 0, entry(2, 2, "b"))).response.getSuccess());
            assertEquals(List.of(entry(1, 1, "a")), node.logEntries());
        } finally { node.stop(); }
    }

    @Test void matchingEntriesArePreservedAndOnlyConflictingSuffixRemoved() throws Exception {
        RaftNode node = node();
        try {
            var first = entry(1, 1, "a"); var second = entry(2, 1, "b");
            assertTrue(send(node, request(3, 0, 0, 0, first, second, entry(3, 2, "wrong"), entry(4, 2, "extra"))).response.getSuccess());
            assertTrue(send(node, request(3, 0, 0, 0, first, second)).response.getSuccess());
            assertEquals(4, node.status().lastLogIndex()); // a matching shorter request cannot erase a suffix
            assertTrue(send(node, request(3, 1, 1, 0, second, entry(3, 3, "right"))).response.getSuccess());
            assertEquals(List.of(first, second, entry(3, 3, "right")), node.logEntries());
            assertTrue(node.appliedCommands().isEmpty());
        } finally { node.stop(); }
    }

    @Test void emptyHeartbeatCannotCommitUnmatchedLocalSuffix() throws Exception {
        RaftNode node = node();
        try {
            send(node, request(2, 0, 0, 0, entry(1, 1, "matched"), entry(2, 1, "unmatched")));
            assertTrue(send(node, request(2, 1, 1, 2)).response.getSuccess());
            assertEquals(1, node.status().commitIndex());
            assertEquals(1, node.status().lastApplied());
            assertEquals(List.of(entry(1, 1, "matched")), node.appliedCommands());
            send(node, request(2, 1, 1, 2));
            assertEquals(1, node.appliedCommands().size());
        } finally { node.stop(); }
    }

    @Test void committedPrefixCannotBeOverwrittenAndApplicationIsIdempotentPerEntry() throws Exception {
        RaftNode node = node();
        try {
            var committed = entry(1, 1, "committed");
            send(node, request(1, 0, 0, 1, committed));
            assertFalse(send(node, request(2, 0, 0, 1, entry(1, 2, "replacement"))).response.getSuccess());
            send(node, request(2, 1, 1, 1));
            assertEquals(List.of(committed), node.logEntries());
            assertEquals(List.of(committed), node.appliedCommands());
            assertEquals(1, node.status().lastApplied());
        } finally { node.stop(); }
    }

    @Test void malformedEntrySequenceIsRejectedBeforeAnyWrite() throws Exception {
        RaftNode node = node();
        try {
            Capture result = send(node, request(3, 0, 0, 0, entry(1, 1, "a"), entry(3, 2, "gap")));
            assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(result.error).getCode());
            assertEquals(0, node.status().lastLogIndex());
            send(node, request(3, 0, 0, 0, entry(1, 1, "a")));
            result = send(node, request(3, 0, 0, 0, entry(1, 1, "different-data")));
            assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(result.error).getCode());
            assertEquals(List.of(entry(1, 1, "a")), node.logEntries());
        } finally { node.stop(); }
    }

    private static final class Capture implements StreamObserver<AppendEntriesResponse> {
        AppendEntriesResponse response; Throwable error;
        public void onNext(AppendEntriesResponse response) { this.response = response; }
        public void onError(Throwable error) { this.error = error; }
        public void onCompleted() {}
    }
}
