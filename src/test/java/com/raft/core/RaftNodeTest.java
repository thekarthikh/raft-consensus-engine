package com.raft.core;

import com.raft.rpc.*;
import com.raft.storage.RaftLog;
import com.raft.storage.RaftPersistentState;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {
    @TempDir Path root;

    private RaftNode node(List<String> members) throws Exception {
        return new RaftNode("node1", 0, members, root, 600, 1200, 100);
    }

    private void seedLog() throws Exception {
        Path directory = root.resolve("node1");
        try (var hardState = new RaftPersistentState(directory)) { hardState.save(4, null); }
        var log = new RaftLog(directory.toString(), "node1");
        try {
            log.append(LogEntry.newBuilder().setIndex(1).setTerm(2).build());
            log.append(LogEntry.newBuilder().setIndex(2).setTerm(4).build());
        } finally { log.close(); }
    }

    private static RequestVoteRequest vote(long term, String candidate, long index, long logTerm) {
        return RequestVoteRequest.newBuilder().setTerm(term).setCandidateId(candidate)
                .setLastLogIndex(index).setLastLogTerm(logTerm).build();
    }

    private static <T> Capture<T> capture() { return new Capture<>(); }
    private static RequestVoteResponse vote(RaftNode node, RequestVoteRequest request) {
        Capture<RequestVoteResponse> observer = capture();
        node.requestVote(request, observer);
        assertNull(observer.error);
        assertTrue(observer.completed);
        return observer.value;
    }
    private static AppendEntriesResponse heartbeat(RaftNode node, long term) {
        Capture<AppendEntriesResponse> observer = capture();
        node.appendEntries(AppendEntriesRequest.newBuilder().setTerm(term).setLeaderId("node2").build(), observer);
        assertNull(observer.error);
        assertTrue(observer.completed);
        return observer.value;
    }
    private static Object field(RaftNode node, String name) throws Exception {
        Field field = RaftNode.class.getDeclaredField(name);
        field.setAccessible(true);
        synchronized (node) { return field.get(node); }
    }
    private static void election(RaftNode node) throws Exception {
        Method method = RaftNode.class.getDeclaredMethod("startElection");
        method.setAccessible(true);
        method.invoke(node);
    }
    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "Condition did not become true within five seconds");
    }
    private static boolean isLeader(RaftNode node) {
        try { return field(node, "state").toString().equals("LEADER"); }
        catch (Exception e) { throw new AssertionError(e); }
    }

    @Test void sameTermHeartbeatCannotEraseVoteIncludingAfterReopen() throws Exception {
        RaftNode node = node(List.of("node1", "node2", "node3"));
        try {
            assertTrue(vote(node, vote(3, "node2", 0, 0)).getVoteGranted());
            assertTrue(heartbeat(node, 3).getSuccess());
            assertFalse(vote(node, vote(3, "node3", 0, 0)).getVoteGranted());
        } finally { node.stop(); }
        node = node(List.of("node1", "node2", "node3"));
        try {
            assertFalse(vote(node, vote(3, "node3", 0, 0)).getVoteGranted());
            assertTrue(vote(node, vote(3, "node2", 0, 0)).getVoteGranted());
            assertTrue(vote(node, vote(4, "node3", 0, 0)).getVoteGranted());
        } finally { node.stop(); }
    }

    @Test void staleRequestsDoNotChangeStateVoteOrTimeout() throws Exception {
        RaftNode node = node(List.of("node1"));
        try {
            assertTrue(vote(node, vote(5, "node2", 0, 0)).getVoteGranted());
            Object deadline = field(node, "electionDeadlineNanos");
            assertFalse(heartbeat(node, 4).getSuccess());
            var response = vote(node, vote(4, "node3", 0, 0));
            assertFalse(response.getVoteGranted());
            assertEquals(5, response.getTerm());
            assertEquals("node2", field(node, "votedFor"));
            assertEquals(deadline, field(node, "electionDeadlineNanos"));
        } finally { node.stop(); }
    }

    @Test void votesCompareLastTermBeforeLastIndex() throws Exception {
        seedLog();
        RaftNode node = node(List.of("node1", "node2"));
        try {
            Object deadline = field(node, "electionDeadlineNanos");
            assertFalse(vote(node, vote(5, "node2", 100, 3)).getVoteGranted());
            assertEquals(5L, field(node, "currentTerm"));
            assertEquals(deadline, field(node, "electionDeadlineNanos"));
            assertFalse(vote(node, vote(5, "node2", 1, 4)).getVoteGranted());
            assertTrue(vote(node, vote(5, "node2", 2, 4)).getVoteGranted());
            assertTrue(vote(node, vote(6, "node3", 1, 5)).getVoteGranted());
        } finally { node.stop(); }
    }

    @Test void automaticElectionTimerElectsSingleMember() throws Exception {
        RaftNode node = node(List.of("node1"));
        try {
            node.start();
            await(() -> isLeader(node));
            assertEquals(1L, field(node, "currentTerm"));
            Thread.sleep(150);
            assertEquals(1L, field(node, "currentTerm"));
        } finally { node.stop(); }
    }

    @Test void staleVoteResponseDoesNotFormMajorityAndHigherResponseStepsDown() throws Exception {
        AtomicInteger behavior = new AtomicInteger(0);
        AtomicReference<RequestVoteRequest> received = new AtomicReference<>();
        Server peer = ServerBuilder.forPort(0).addService(new RaftServiceGrpc.RaftServiceImplBase() {
            @Override public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
                received.set(request);
                long responseTerm = behavior.get() == 0 ? request.getTerm() - 1 : request.getTerm() + 2;
                observer.onNext(RequestVoteResponse.newBuilder().setTerm(responseTerm).setVoteGranted(true).build());
                observer.onCompleted();
            }
        }).build().start();
        RaftNode node = new RaftNode("node1", 0, List.of("node1", "127.0.0.1:" + peer.getPort()),
                root, 3000, 6000, 500);
        try {
            node.start();
            election(node);
            await(() -> received.get() != null);
            Thread.sleep(100);
            assertEquals("CANDIDATE", field(node, "state").toString());
            assertEquals(1L, field(node, "currentTerm"));
            assertNull(field(node, "heartbeatTask"));
            behavior.set(1);
            received.set(null);
            election(node);
            await(() -> {
                try { return field(node, "currentTerm").equals(4L); }
                catch (Exception e) { throw new AssertionError(e); }
            });
            assertEquals("FOLLOWER", field(node, "state").toString());
            assertNull(field(node, "votedFor"));
            assertNull(field(node, "heartbeatTask"));
        } finally { node.stop(); peer.shutdownNow().awaitTermination(3, TimeUnit.SECONDS); }
        try (var state = new RaftPersistentState(root.resolve("node1"))) {
            assertEquals(4, state.currentTerm());
            assertNull(state.votedFor());
        }
    }

    @Test void singleMemberElectsSelfAndPersistsSelfVote() throws Exception {
        RaftNode node = node(List.of("node1", "node1"));
        try {
            election(node);
            assertTrue(isLeader(node));
            assertEquals("node1", field(node, "votedFor"));
            assertFalse(vote(node, vote(1, "node2", 0, 0)).getVoteGranted());
            assertTrue(heartbeat(node, 1).getSuccess());
            assertEquals("node1", field(node, "votedFor"));
            assertNull(field(node, "heartbeatTask"));
        } finally { node.stop(); }
        try (var state = new RaftPersistentState(root.resolve("node1"))) {
            assertEquals(1, state.currentTerm());
            assertEquals("node1", state.votedFor());
        }
    }

    @Test void timeoutIsSampledOnceAndResetByValidLeaderContact() throws Exception {
        RaftNode node = node(List.of("node1"));
        try {
            long before = System.nanoTime();
            heartbeat(node, 1);
            long deadline = (long) field(node, "electionDeadlineNanos");
            long after = System.nanoTime();
            assertTrue(deadline >= before + TimeUnit.MILLISECONDS.toNanos(600));
            assertTrue(deadline < after + TimeUnit.MILLISECONDS.toNanos(1200));
            Thread.sleep(40);
            assertEquals(deadline, field(node, "electionDeadlineNanos"));
            heartbeat(node, 1);
            assertNotEquals(deadline, field(node, "electionDeadlineNanos"));
            election(node);
            assertTrue((long) field(node, "electionDeadlineNanos") > System.nanoTime());
        } finally { node.stop(); }
    }

    @Test void concurrentCandidatesCannotBothReceiveSameTermVote() throws Exception {
        RaftNode node = node(List.of("node1", "node2", "node3"));
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch gate = new CountDownLatch(1);
            Future<Boolean> first = workers.submit(() -> { gate.await(); return vote(node, vote(2, "node2", 0, 0)).getVoteGranted(); });
            Future<Boolean> second = workers.submit(() -> { gate.await(); return vote(node, vote(2, "node3", 0, 0)).getVoteGranted(); });
            gate.countDown();
            assertNotEquals(first.get(3, TimeUnit.SECONDS), second.get(3, TimeUnit.SECONDS));
        } finally { workers.shutdownNow(); node.stop(); }
    }

    @Test void persistenceFailureDoesNotAcknowledgeVoteAndStopsNode() throws Exception {
        RaftNode node = node(List.of("node1"));
        try {
            Files.createDirectory(root.resolve("node1/hard-state.tmp"));
            Capture<RequestVoteResponse> observer = capture();
            node.requestVote(vote(1, "node2", 0, 0), observer);
            assertNotNull(observer.error);
            assertNull(observer.value);
            assertFalse((boolean) field(node, "running"));
        } finally { node.stop(); }
    }

    @Test void entriesAreStoredButNotAppliedBeforeLeaderCommit() throws Exception {
        RaftNode node = node(List.of("node1"));
        try {
            Capture<AppendEntriesResponse> observer = capture();
            node.appendEntries(AppendEntriesRequest.newBuilder().setTerm(1).setLeaderId("node2")
                    .addEntries(LogEntry.newBuilder().setTerm(1).setIndex(1)).build(), observer);
            assertTrue(observer.value.getSuccess());
            assertEquals(1, observer.value.getLastLogIndex());
            assertEquals(0, node.status().commitIndex());
            assertTrue(node.appliedCommands().isEmpty());
        } finally { node.stop(); }
    }

    @Test void realGrpcElectionUsesWalMetadataAndCancelsOldHeartbeats() throws Exception {
        seedLog();
        AtomicReference<RequestVoteRequest> received = new AtomicReference<>();
        AtomicInteger heartbeats = new AtomicInteger();
        Server peer = ServerBuilder.forPort(0).addService(new RaftServiceGrpc.RaftServiceImplBase() {
            @Override public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
                received.set(request);
                observer.onNext(RequestVoteResponse.newBuilder().setTerm(request.getTerm()).setVoteGranted(true).build());
                observer.onCompleted();
            }
            @Override public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
                heartbeats.incrementAndGet();
                observer.onNext(AppendEntriesResponse.newBuilder().setTerm(request.getTerm()).setSuccess(true)
                        .setLastLogIndex(request.getPrevLogIndex() + request.getEntriesCount()).build());
                observer.onCompleted();
            }
        }).build().start();
        String endpoint = "127.0.0.1:" + peer.getPort();
        RaftNode node = node(List.of("node1", endpoint, endpoint));
        ManagedChannel client = null;
        try {
            node.start();
            election(node);
            await(() -> isLeader(node) && heartbeats.get() > 0);
            await(() -> node.status().matchIndex().getOrDefault(endpoint, 0L) == 3);
            assertEquals(2, received.get().getLastLogIndex());
            assertEquals(4, received.get().getLastLogTerm());
            long leaderTerm = received.get().getTerm();
            assertTrue(leaderTerm >= 5); // Cold channel setup may legitimately require an election retry.
            assertEquals(leaderTerm, field(node, "currentTerm"));
            assertEquals(4L, ((Map<?, ?>) field(node, "nextIndex")).get(endpoint));
            assertEquals(3L, ((Map<?, ?>) field(node, "matchIndex")).get(endpoint));
            ScheduledFuture<?> oldTask = (ScheduledFuture<?>) field(node, "heartbeatTask");
            Server nodeServer = (Server) field(node, "server");
            client = ManagedChannelBuilder.forAddress("127.0.0.1", nodeServer.getPort()).usePlaintext().build();
            var response = RaftServiceGrpc.newBlockingStub(client).withDeadlineAfter(2, TimeUnit.SECONDS)
                    .appendEntries(AppendEntriesRequest.newBuilder().setTerm(leaderTerm + 1).setLeaderId("node2").build());
            assertTrue(response.getSuccess());
            assertTrue(oldTask.isCancelled());
            assertNull(field(node, "heartbeatTask"));
            assertTrue(((Map<?, ?>) field(node, "nextIndex")).isEmpty());
            Thread.sleep(150); // Allow already delivered RPCs to finish before checking no new sends.
            int count = heartbeats.get();
            Thread.sleep(150); // Below the minimum election timeout.
            assertEquals(count, heartbeats.get());
            election(node);
            await(() -> isLeader(node));
            assertNotSame(oldTask, field(node, "heartbeatTask"));
            assertTrue(oldTask.isCancelled());
        } finally {
            if (client != null) client.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
            node.stop();
            peer.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    private static final class Capture<T> implements StreamObserver<T> {
        T value;
        Throwable error;
        boolean completed;
        @Override public void onNext(T value) { this.value = value; }
        @Override public void onError(Throwable error) { this.error = error; }
        @Override public void onCompleted() { completed = true; }
    }
}
