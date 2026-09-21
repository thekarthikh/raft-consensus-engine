package com.raft.core;

import com.google.protobuf.ByteString;
import com.raft.rpc.*;
import com.raft.storage.RaftLog;
import com.raft.storage.RaftPersistentState;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

class RaftReplicationIntegrationTest {
    @TempDir Path root;

    static LogEntry entry(long index, long term, String command) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term).setData(ByteString.copyFromUtf8(command)).build();
    }

    private void seed(int number, long term, List<LogEntry> entries) throws Exception {
        String id = "node" + number;
        Path directory = root.resolve(id);
        try (var state = new RaftPersistentState(directory)) { state.save(term, null); }
        RaftLog log = new RaftLog(directory.toString(), id);
        try { log.appendAll(entries); } finally { log.close(); }
    }

    static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(condition.getAsBoolean(), "Condition not satisfied within 12 seconds");
    }

    static List<String> commands(RaftNode node) {
        return node.appliedCommands().stream().map(e -> e.getData().toStringUtf8()).toList();
    }

    @Test void threeActualNodesReplicateCommitAndApplyClientCommandInOrder() throws Exception {
        try (Cluster cluster = new Cluster(null)) {
            cluster.start();
            cluster.awaitLeader();
            var response = cluster.submit(0, "first", 4);
            assertTrue(response.getCommitted());
            assertEquals("node1", response.getLeaderId());
            assertEquals(2, response.getIndex()); // index 1 is the leadership no-op
            assertTrue(cluster.submit(0, "second", 4).getCommitted());
            await(() -> cluster.allApplied(List.of("first", "second")));
            for (RaftNode node : cluster.nodes) {
                assertEquals(3, node.status().commitIndex());
                assertEquals(3, node.status().lastApplied());
                assertEquals(cluster.nodes[0].logEntries(), node.logEntries());
            }
            await(() -> cluster.nodes[0].status().matchIndex().values().stream().allMatch(i -> i >= 3));
            System.out.println("ACTUAL THREE-NODE RESULT: leader=node1, commandIndices=2,3, "
                    + "all commitIndex=3, all lastApplied=3, applied=[first, second]");
        }
    }

    @Test void followerRejectsClientWriteAndReportsKnownLeader() throws Exception {
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            await(() -> cluster.nodes[1].status().leaderId().equals("node1"));
            long index = cluster.nodes[1].status().lastLogIndex();
            var response = cluster.submit(1, "not-a-leader-write", 3);
            assertFalse(response.getCommitted());
            assertEquals("node1", response.getLeaderId());
            assertEquals(0, response.getIndex());
            assertEquals(index, cluster.nodes[1].status().lastLogIndex());
            assertTrue(cluster.nodes[1].appliedCommands().isEmpty());
        }
    }

    @Test void survivingActualNodesElectNewLeaderAndContinueCommittedHistory() throws Exception {
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            assertTrue(cluster.submit(0, "before-failover", 4).getCommitted());
            await(() -> cluster.allApplied(List.of("before-failover")));
            long oldTerm = cluster.nodes[0].status().term();
            cluster.stop(0);
            await(() -> cluster.nodes[1].status().role().equals("LEADER") || cluster.nodes[2].status().role().equals("LEADER"));
            int leader = cluster.nodes[1].status().role().equals("LEADER") ? 1 : 2;
            assertTrue(cluster.nodes[leader].status().term() > oldTerm);
            assertTrue(cluster.submit(leader, "after-failover", 4).getCommitted());
            await(() -> commands(cluster.nodes[1]).equals(List.of("before-failover", "after-failover"))
                    && commands(cluster.nodes[2]).equals(List.of("before-failover", "after-failover")));
            assertEquals(4, cluster.nodes[1].status().lastApplied());
            assertEquals(4, cluster.nodes[2].status().lastApplied());
        }
    }

    @Test void oneUnavailableFollowerDoesNotPreventQuorumAndLaterCatchesUp() throws Exception {
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            await(() -> cluster.nodes[2].status().commitIndex() == 1);
            cluster.stop(2);
            for (String command : List.of("a", "b", "c")) assertTrue(cluster.submit(0, command, 4).getCommitted());
            assertEquals(List.of("a", "b", "c"), commands(cluster.nodes[0]));
            cluster.restart(2);
            await(() -> cluster.allApplied(List.of("a", "b", "c")));
            assertEquals(cluster.nodes[0].logEntries(), cluster.nodes[2].logEntries());
            await(() -> cluster.nodes[0].status().matchIndex().get("node3") == 4);
            assertEquals(5, cluster.nodes[0].status().nextIndex().get("node3"));
        }
    }

    @Test void leaderAloneCannotCommitAndReturningFollowerRestoresQuorum() throws Exception {
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            await(() -> cluster.nodes[1].status().commitIndex() == 1 && cluster.nodes[2].status().commitIndex() == 1);
            cluster.stop(1); cluster.stop(2);
            StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> cluster.submit(0, "pending", 1));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, error.getStatus().getCode());
            assertEquals(2, cluster.nodes[0].status().lastLogIndex());
            assertEquals(1, cluster.nodes[0].status().commitIndex());
            assertEquals(1, cluster.nodes[0].status().lastApplied());
            assertTrue(cluster.nodes[0].appliedCommands().isEmpty());
            cluster.restart(1);
            await(() -> commands(cluster.nodes[0]).equals(List.of("pending"))
                    && commands(cluster.nodes[1]).equals(List.of("pending")));
            assertEquals(2, cluster.nodes[0].status().commitIndex());
            // Timeout meant unknown outcome, not rejection: the original entry commits later without resubmission.
        }
    }

    @Test void conflictingSuffixIsReplacedWithoutRemovingMatchingPrefix() throws Exception {
        var shared = entry(1, 2, "shared");
        seed(1, 5, List.of(shared));
        seed(2, 5, List.of(shared, entry(2, 4, "wrong-a"), entry(3, 4, "wrong-b")));
        seed(3, 5, List.of(shared));
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            assertTrue(cluster.submit(0, "right", 4).getCommitted());
            await(() -> cluster.allApplied(List.of("shared", "right")));
            assertEquals(shared, cluster.nodes[1].logEntries().get(0));
            assertEquals(cluster.nodes[0].logEntries(), cluster.nodes[1].logEntries());
            assertFalse(cluster.nodes[1].logEntries().stream().anyMatch(e -> e.getData().toStringUtf8().startsWith("wrong")));
        }
    }

    @Test void missingAndConflictingPreviousEntriesAreBacktrackedDuringInitialCatchup() throws Exception {
        var history = List.of(entry(1, 2, "one"), entry(2, 2, "two"), entry(3, 2, "three"));
        seed(1, 3, history);
        seed(2, 3, List.of(entry(1, 1, "wrong")));
        seed(3, 3, List.of());
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            await(() -> cluster.allApplied(List.of("one", "two", "three")));
            for (RaftNode node : cluster.nodes) assertEquals(cluster.nodes[0].logEntries(), node.logEntries());
        }
    }

    @Test void staleAppendRejectedAndNewerTermStopsActualLeader() throws Exception {
        try (Cluster cluster = new Cluster(null)) {
            cluster.start(); cluster.awaitLeader();
            long term = cluster.nodes[0].status().term();
            var stale = cluster.stub(1).appendEntries(AppendEntriesRequest.newBuilder().setTerm(term - 1).setLeaderId("node1").build());
            assertFalse(stale.getSuccess());
            assertEquals(term, stale.getTerm());
            var newer = cluster.stub(0).appendEntries(AppendEntriesRequest.newBuilder().setTerm(term + 1).setLeaderId("node2").build());
            assertTrue(newer.getSuccess());
            assertEquals("FOLLOWER", cluster.nodes[0].status().role());
            assertEquals(term + 1, cluster.nodes[0].status().term());
            assertFalse(cluster.submit(0, "wrong-term-write", 3).getCommitted());
        }
    }

    @Test void newerTermFollowerResponseAbortsPendingClientInsteadOfFalseCommit() throws Exception {
        BatchBarrier barrier = new BatchBarrier(1, request -> request.getEntriesList().stream()
                .anyMatch(e -> e.getData().toStringUtf8().equals("wait")));
        var writer = Executors.newSingleThreadExecutor();
        try (Cluster cluster = new Cluster(barrier)) {
            cluster.start(); cluster.awaitLeader();
            await(() -> cluster.nodes[1].status().commitIndex() == 1 && cluster.nodes[2].status().commitIndex() == 1);
            cluster.stop(2);
            var client = writer.submit(() -> cluster.submit(0, "wait", 5));
            assertTrue(barrier.blocked.await(5, TimeUnit.SECONDS));
            long oldTerm = cluster.nodes[0].status().term();
            assertTrue(cluster.stub(1).appendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(oldTerm + 1).setLeaderId("node3").build()).getSuccess());
            barrier.release.countDown();
            ExecutionException error = assertThrows(ExecutionException.class, () -> client.get(5, TimeUnit.SECONDS));
            assertEquals(Status.Code.ABORTED, ((StatusRuntimeException) error.getCause()).getStatus().getCode());
            assertEquals("FOLLOWER", cluster.nodes[0].status().role());
            assertEquals(oldTerm + 1, cluster.nodes[0].status().term());
            assertEquals(1, cluster.nodes[0].status().commitIndex());
            assertTrue(cluster.nodes[0].appliedCommands().isEmpty());
        } finally { barrier.release.countDown(); writer.shutdownNow(); }
    }

    @Test void majorityOfOlderTermEntriesDoesNotCommitUntilCurrentTermEntryReplicates() throws Exception {
        List<LogEntry> history = new ArrayList<>();
        for (int i = 1; i <= 100; i++) history.add(entry(i, 2, "old-" + i));
        seed(1, 2, history); seed(2, 2, List.of()); seed(3, 2, List.of());
        BatchBarrier barrier = new BatchBarrier();
        try (Cluster cluster = new Cluster(barrier)) {
            cluster.start(); cluster.awaitLeader();
            assertTrue(barrier.blocked.await(12, TimeUnit.SECONDS));
            assertEquals(64, cluster.nodes[1].status().lastLogIndex());
            assertEquals(64, cluster.nodes[2].status().lastLogIndex());
            assertEquals(64, cluster.nodes[0].status().matchIndex().get("node2"));
            assertEquals(64, cluster.nodes[0].status().matchIndex().get("node3"));
            assertEquals(0, cluster.nodes[0].status().commitIndex());
            assertEquals(0, cluster.nodes[0].status().lastApplied());
            assertTrue(cluster.nodes[0].appliedCommands().isEmpty());
            barrier.release.countDown();
            await(() -> cluster.nodes[0].status().commitIndex() == 101
                    && cluster.nodes[1].status().lastApplied() == 101 && cluster.nodes[2].status().lastApplied() == 101);
            for (RaftNode node : cluster.nodes) assertEquals(history, node.appliedCommands());
        } finally { barrier.release.countDown(); }
    }

    private static final class BatchBarrier {
        final CountDownLatch blocked;
        final CountDownLatch release = new CountDownLatch(1);
        final Predicate<AppendEntriesRequest> shouldBlock;
        BatchBarrier() { this(2, request -> request.getPrevLogIndex() == 64
                && request.getEntriesList().stream().anyMatch(LogEntry::getNoOp)); }
        BatchBarrier(int nodes, Predicate<AppendEntriesRequest> shouldBlock) {
            this.blocked = new CountDownLatch(nodes);
            this.shouldBlock = shouldBlock;
        }
    }

    /** Only holds delivery of the second batch; all log, term, commit and application behavior is real RaftNode code. */
    private final class DelayedNode extends RaftNode {
        private final BatchBarrier barrier;
        private final AtomicBoolean reachedBarrier = new AtomicBoolean();
        DelayedNode(String id, int port, List<String> members, BatchBarrier barrier) throws Exception {
            super(id, port, members, root, 6000, 9000, 100);
            this.barrier = barrier;
        }
        @Override public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
            // A backtracking probe can have the same prevLogIndex before this prefix exists.
            // Delay only a batch whose previous prefix has actually been stored.
            if (barrier.shouldBlock.test(request) && request.getPrevLogIndex() <= status().lastLogIndex()) {
                if (reachedBarrier.compareAndSet(false, true)) barrier.blocked.countDown();
                try { barrier.release.await(15, TimeUnit.SECONDS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); observer.onError(Status.CANCELLED.asRuntimeException()); return; }
                if (Context.current().isCancelled()) return;
            }
            super.appendEntries(request, observer);
        }
    }

    private final class Cluster implements AutoCloseable {
        final RaftNode[] nodes = new RaftNode[3];
        final ManagedChannel[] clients = new ManagedChannel[3];
        final int[] ports = new int[3];
        final List<String> members;
        final BatchBarrier barrier;

        Cluster(BatchBarrier barrier) throws Exception {
            this.barrier = barrier;
            List<ServerSocket> reservations = new ArrayList<>();
            try {
                for (int i = 0; i < 3; i++) { ServerSocket socket = new ServerSocket(0); reservations.add(socket); ports[i] = socket.getLocalPort(); }
            } finally { for (ServerSocket socket : reservations) socket.close(); }
            members = List.of("node1=127.0.0.1:" + ports[0], "node2=127.0.0.1:" + ports[1], "node3=127.0.0.1:" + ports[2]);
            for (int i = 0; i < 3; i++) {
                nodes[i] = create(i);
                clients[i] = ManagedChannelBuilder.forAddress("127.0.0.1", ports[i]).usePlaintext().build();
            }
        }
        RaftNode create(int i) throws Exception {
            if (barrier != null && i != 0) return new DelayedNode("node" + (i + 1), ports[i], members, barrier);
            return new RaftNode("node" + (i + 1), ports[i], members, root, i == 0 ? 600 : 6000, i == 0 ? 1200 : 9000, 100);
        }
        void start() throws Exception { for (RaftNode node : nodes) node.start(); }
        void awaitLeader() throws Exception { await(() -> nodes[0].status().role().equals("LEADER")); }
        RaftServiceGrpc.RaftServiceBlockingStub stub(int i) { return RaftServiceGrpc.newBlockingStub(clients[i]).withDeadlineAfter(4, TimeUnit.SECONDS); }
        SubmitCommandResponse submit(int i, String data, long timeoutSeconds) {
            return RaftServiceGrpc.newBlockingStub(clients[i]).withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                    .submitCommand(SubmitCommandRequest.newBuilder().setData(ByteString.copyFromUtf8(data)).build());
        }
        void stop(int i) throws Exception { nodes[i].stop(); nodes[i].blockUntilShutdown(); }
        void restart(int i) throws Exception { nodes[i] = create(i); nodes[i].start(); }
        boolean allApplied(List<String> expected) {
            for (RaftNode node : nodes) if (!commands(node).equals(expected)) return false;
            return true;
        }
        @Override public void close() throws Exception {
            if (barrier != null) barrier.release.countDown();
            for (RaftNode node : nodes) if (node != null) node.stop();
            for (ManagedChannel client : clients) if (client != null) client.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }
}
