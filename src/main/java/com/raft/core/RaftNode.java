package com.raft.core;

import com.raft.rpc.AppendEntriesRequest;
import com.raft.rpc.AppendEntriesResponse;
import com.raft.rpc.RaftServiceGrpc;
import com.raft.rpc.RequestVoteRequest;
import com.raft.rpc.RequestVoteResponse;
import com.raft.rpc.LogEntry;
import com.raft.rpc.SubmitCommandRequest;
import com.raft.rpc.SubmitCommandResponse;
import com.raft.storage.RaftLog;
import com.raft.storage.RaftPersistentState;
import io.grpc.ManagedChannel;
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode extends RaftServiceGrpc.RaftServiceImplBase {
    private static final int APPEND_BATCH_SIZE = 64;
    private static final int MAX_COMMAND_BYTES = 32 * 1024;

    private static final Logger logger =
            LoggerFactory.getLogger(RaftNode.class);

    private enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private final String nodeId;
    private final int port;
    private final List<String> peers;
    private final Map<String, String> peerAddresses;

    private volatile State state = State.FOLLOWER;
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private long electionDeadlineNanos;
    private final long electionMinMillis;
    private final long electionMaxMillis;
    private final long heartbeatMillis;
    private final RaftPersistentState persistentState;
    private final RaftLog raftLog;
    // Volatile Raft state, reconstructed through the leader's commit information on restart.
    private long commitIndex;
    private long lastApplied;
    private String leaderId = "";
    private final CommittedStateMachine stateMachine = new CommittedStateMachine();
    private final Map<Long, CompletableFuture<SubmitCommandResponse>> pendingCommands = new HashMap<>();
    // Volatile replication progress: initialized on leadership, not persisted.
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();
    private ScheduledFuture<?> electionTask;
    private ScheduledFuture<?> heartbeatTask;
    private final Set<Context.CancellableContext> activeHeartbeats = new HashSet<>();
    private final Map<String, Long> replicationInFlight = new HashMap<>();
    private long leadershipGeneration;
    private boolean started;

    private Server server;
    private Thread shutdownHook;

    private final Map<String, ManagedChannel> channels =
            new ConcurrentHashMap<>();

    private final Map<String, RaftServiceGrpc.RaftServiceBlockingStub> stubs =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(8);

    private final Random random = new Random();

    private volatile boolean running = true;
    public RaftNode(String nodeId, int port, List<String> peers) throws IOException {
        this(nodeId, port, peers, Path.of(System.getProperty("raft.dataDir", "data")),
                Long.getLong("raft.electionMinMillis", 1500L),
                Long.getLong("raft.electionMaxMillis", 3000L),
                Long.getLong("raft.heartbeatMillis", 500L));
    }

    public RaftNode(String nodeId, int port, List<String> peers, Path dataRoot,
                    long electionMinMillis, long electionMaxMillis, long heartbeatMillis) throws IOException {
        if (nodeId == null || !nodeId.matches("[A-Za-z0-9_-]+"))
            throw new IllegalArgumentException("Invalid node ID");
        if (heartbeatMillis <= 0 || electionMinMillis < 3 * heartbeatMillis
                || electionMaxMillis <= electionMinMillis)
            throw new IllegalArgumentException("Election range must exceed three heartbeat intervals");
        this.nodeId = nodeId;
        this.port = port;
        // Backwards compatible IDs/host:port, plus id=host:port for multiple local nodes.
        Map<String, String> members = new LinkedHashMap<>();
        for (String member : peers) {
            if (member == null || member.isBlank()) throw new IllegalArgumentException("Invalid member");
            String[] parts = member.split("=", 2);
            String id = parts[0].trim();
            String address = parts.length == 2 ? parts[1].trim() : id;
            if (id.isEmpty() || address.isEmpty()) throw new IllegalArgumentException("Invalid member");
            String previous = members.putIfAbsent(id, address);
            if (previous != null && !previous.equals(address))
                throw new IllegalArgumentException("Conflicting addresses for member " + id);
        }
        members.putIfAbsent(nodeId, nodeId);
        if (new HashSet<>(members.values()).size() != members.size())
            throw new IllegalArgumentException("Members must have distinct endpoint addresses");
        this.peers = new ArrayList<>(members.keySet());
        this.peerAddresses = Map.copyOf(members);
        this.electionMinMillis = electionMinMillis;
        this.electionMaxMillis = electionMaxMillis;
        this.heartbeatMillis = heartbeatMillis;
        Path directory = dataRoot.resolve(nodeId);
        Path legacyLog = dataRoot.resolve("raft_log_" + nodeId + ".bin");
        if (Files.exists(legacyLog) && Files.size(legacyLog) > 0)
            throw new IOException("Legacy WAL requires explicit migration before startup: " + legacyLog);
        persistentState = new RaftPersistentState(directory);
        RaftLog openedLog = null;
        try {
            openedLog = new RaftLog(directory.toString(), nodeId);
            if (openedLog.getLastTerm() > persistentState.currentTerm())
                throw new IOException("WAL term exceeds persistent currentTerm");
        } catch (IOException | RuntimeException e) {
            if (openedLog != null) openedLog.close();
            persistentState.close();
            throw e;
        }
        raftLog = openedLog;
        currentTerm = persistentState.currentTerm();
        votedFor = persistentState.votedFor();
        resetElectionTimeout();
    }

    public synchronized void start() throws IOException {
        if (started || !running) throw new IllegalStateException("Node cannot be started twice");
        started = true;

        try {
        server = ServerBuilder
                .forPort(port)
                .addService(this)
                .build()
                .start();

        logger.info(
                "Node {} gRPC server started on port {}",
                nodeId,
                server.getPort()
        );

        shutdownHook = new Thread(this::stop, "raft-shutdown-" + nodeId);
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        connectToPeers();
        resetElectionTimeout();
        startElectionTimer();
        logger.info("Election timeout {}-{} ms; heartbeat {} ms", electionMinMillis,
                electionMaxMillis, heartbeatMillis);
        } catch (IOException | RuntimeException e) {
            stop();
            throw e;
        }
    }

    private void connectToPeers() {

        for (String peer : peers) {

            if (peer.equals(nodeId)) {
                continue;
            }

            String address = peerAddresses.get(peer);
            String host = address;
            int peerPort = 50051;

            if (address.contains(":")) {
                String[] parts = address.split(":");
                host = parts[0];
                peerPort = Integer.parseInt(parts[1]);
            }

            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(host, peerPort)
                    .usePlaintext()
                    .build();

            channels.put(peer, channel);

            RaftServiceGrpc.RaftServiceBlockingStub stub =
                    RaftServiceGrpc.newBlockingStub(channel);

            stubs.put(peer, stub);

            logger.info(
                    "Node {} connected to peer {}:{}",
                    nodeId,
                    host,
                    peerPort
            );
        }
    }

    private void startElectionTimer() {
        electionTask = scheduler.scheduleWithFixedDelay(() -> {
            synchronized (RaftNode.this) {
                if (running && state != State.LEADER
                        && System.nanoTime() - electionDeadlineNanos >= 0) startElection();
            }
        }, 25, 25, TimeUnit.MILLISECONDS);
    }

    private void resetElectionTimeout() {
        long timeout = random.nextLong(electionMinMillis, electionMaxMillis);
        electionDeadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    private void persist(long term, String vote) throws IOException {
        persistentState.save(term, vote);
        currentTerm = term;
        votedFor = vote;
    }

    private void cancelHeartbeats() {
        leadershipGeneration++;
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        heartbeatTask = null;
        for (Context.CancellableContext call : activeHeartbeats) call.cancel(null);
        activeHeartbeats.clear();
        replicationInFlight.clear();
        nextIndex.clear();
        matchIndex.clear();
    }

    private void becomeFollower(long term) throws IOException {
        boolean wasLeader = state == State.LEADER;
        if (term > currentTerm) persist(term, null);
        // Same-term step-down must retain the vote already cast in this term.
        state = State.FOLLOWER;
        leaderId = "";
        failPending(Status.ABORTED.withDescription("Leadership changed; command outcome is unknown"));
        cancelHeartbeats();
        if (wasLeader) resetElectionTimeout();
    }

    private void persistenceFailed(IOException error) {
        logger.error("Node {} stopping after persistent-state failure", nodeId, error);
        stop();
    }

    private synchronized void startElection() {

        if (!running || state == State.LEADER) {
            return;
        }
        if (currentTerm == Long.MAX_VALUE) {
            logger.error("Node {} exhausted the term range; stopping", nodeId);
            stop();
            return;
        }
        try {
            persist(currentTerm + 1, nodeId);
        } catch (IOException e) {
            persistenceFailed(e);
            return;
        }
        state = State.CANDIDATE;
        leaderId = "";
        resetElectionTimeout();

        long electionTerm = currentTerm;

        AtomicInteger votes = new AtomicInteger(1);

        int majority = (peers.size() / 2) + 1;
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(electionTerm).setCandidateId(nodeId)
                .setLastLogIndex(raftLog.getLastIndex()).setLastLogTerm(raftLog.getLastTerm()).build();
        if (votes.get() >= majority) {
            becomeLeader();
            return;
        }

        logger.info(
                "Node {} starting election for term {}",
                nodeId,
                electionTerm
        );

        for (String peer : peers) {

            if (peer.equals(nodeId)) {
                continue;
            }

            scheduler.execute(() -> {

                try {

                    RaftServiceGrpc.RaftServiceBlockingStub stub =
                            stubs.get(peer);

                    synchronized (RaftNode.this) {
                        if (!running || state != State.CANDIDATE || currentTerm != electionTerm) return;
                    }

                    RequestVoteResponse response =
                            stub
                                    .withDeadlineAfter(
                                            electionMinMillis / 2,
                                            TimeUnit.MILLISECONDS
                                    )
                                    .requestVote(request);

                    synchronized (RaftNode.this) {
                        if (!running) return;

                        if (response.getTerm() > currentTerm) {

                            becomeFollower(response.getTerm());
                            resetElectionTimeout();

                            return;
                        }

                        if (state == State.CANDIDATE
                                && currentTerm == electionTerm
                                && response.getTerm() == electionTerm
                                && response.getVoteGranted()) {

                            int voteCount =
                                    votes.incrementAndGet();

                            logger.info(
                                    "Node {} received vote from {}. Votes: {}",
                                    nodeId,
                                    peer,
                                    voteCount
                            );

                            if (voteCount >= majority) {
                                becomeLeader();
                            }
                        }
                    }

                } catch (IOException e) {
                    synchronized (RaftNode.this) { persistenceFailed(e); }
                } catch (Exception e) {

                    logger.error(
                            "Node {} failed to get vote from {}",
                            nodeId,
                            peer,
                            e
                    );
                }
            });
        }

    }

    private synchronized void becomeLeader() {

        if (!running || state != State.CANDIDATE) {
            return;
        }
        cancelHeartbeats();
        state = State.LEADER;
        leaderId = nodeId;
        for (String peer : peers) {
            if (!peer.equals(nodeId)) {
                nextIndex.put(peer, raftLog.getLastIndex() + 1);
                matchIndex.put(peer, 0L);
            }
        }
        long leaderTerm = currentTerm;
        long generation = leadershipGeneration;
        try {
            // A current-term entry permits commitment of a replicated earlier-term prefix.
            raftLog.append(LogEntry.newBuilder().setIndex(raftLog.getLastIndex() + 1)
                    .setTerm(currentTerm).setNoOp(true).build());
            advanceLeaderCommit();
        } catch (IOException e) {
            persistenceFailed(e);
            return;
        }

        logger.info("==============================");
        logger.info(
                "Node {} BECAME LEADER for term {}",
                nodeId,
                currentTerm
        );
        logger.info("==============================");

        heartbeatTask = scheduler.scheduleAtFixedRate(
                () -> sendHeartbeats(leaderTerm, generation),
                0,
                heartbeatMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private synchronized void sendHeartbeats(long leaderTerm, long generation) {
        if (!running || state != State.LEADER || currentTerm != leaderTerm
                || leadershipGeneration != generation) {
            return;
        }
        for (String peer : peers) {
            if (peer.equals(nodeId) || replicationInFlight.containsKey(peer)) continue;
            Context.CancellableContext call = Context.ROOT.withCancellation();
            activeHeartbeats.add(call);
            replicationInFlight.put(peer, generation);
            scheduler.execute(() -> replicateToPeer(peer, leaderTerm, generation, call));
        }
    }

    /** One worker per follower per leadership generation; network I/O is outside the node lock. */
    private void replicateToPeer(String peer, long leaderTerm, long generation,
                                 Context.CancellableContext call) {
        try {
            while (true) {
                AppendEntriesRequest request;
                synchronized (this) {
                    if (!isCurrentLeader(leaderTerm, generation)) return;
                    long next = nextIndex.get(peer);
                    long previous = next - 1;
                    request = AppendEntriesRequest.newBuilder().setTerm(leaderTerm).setLeaderId(nodeId)
                            .setPrevLogIndex(previous).setPrevLogTerm(raftLog.getTerm(previous))
                            .addAllEntries(raftLog.getEntriesFrom(next, APPEND_BATCH_SIZE))
                            .setLeaderCommit(commitIndex).build();
                }
                AppendEntriesResponse response = call.call(() -> stubs.get(peer)
                        .withDeadlineAfter(electionMinMillis / 2, TimeUnit.MILLISECONDS).appendEntries(request));
                synchronized (this) {
                    if (!running) return;
                    if (response.getTerm() > currentTerm) {
                        becomeFollower(response.getTerm());
                        resetElectionTimeout();
                        return;
                    }
                    if (!isCurrentLeader(leaderTerm, generation)) return;
                    if (response.getTerm() != leaderTerm) {
                        logger.warn("Node {} ignored stale AppendEntries response from {}", nodeId, peer);
                        return;
                    }
                    long acknowledged = request.getPrevLogIndex() + request.getEntriesCount();
                    if (response.getSuccess()) {
                        if (response.getLastLogIndex() < acknowledged) {
                            logger.error("Peer {} reported success without the acknowledged prefix {}", peer, acknowledged);
                            return;
                        }
                        // Do not count an unrelated suffix reported by the follower as replicated.
                        matchIndex.put(peer, Math.max(matchIndex.get(peer), acknowledged));
                        nextIndex.put(peer, matchIndex.get(peer) + 1);
                        advanceLeaderCommit();
                        if (acknowledged == raftLog.getLastIndex() && request.getLeaderCommit() == commitIndex) return;
                        // More entries or a new commit notice: continue on this same serialized worker.
                    } else {
                        long next = nextIndex.get(peer);
                        long floor = matchIndex.get(peer) + 1;
                        if (next <= floor) {
                            logger.error("Peer {} rejected its previously acknowledged prefix (nextIndex={})", peer, next);
                            return;
                        }
                        // A consistency rejection reduces the search by a log position, not an arbitrary retry count.
                        nextIndex.put(peer, next - 1);
                    }
                }
            }
        } catch (IOException e) {
            synchronized (this) { persistenceFailed(e); }
        } catch (Exception e) {
            if (!call.isCancelled())
                logger.warn("Node {} replication to {} failed: {}", nodeId, peer, e.toString());
            // Unavailability/timeouts leave progress unchanged. The heartbeat scheduler retries later.
        } finally {
            call.cancel(null);
            synchronized (this) {
                activeHeartbeats.remove(call);
                replicationInFlight.remove(peer, generation);
            }
        }
    }

    private boolean isCurrentLeader(long term, long generation) {
        return running && state == State.LEADER && currentTerm == term && leadershipGeneration == generation;
    }

    private void advanceLeaderCommit() {
        List<Long> replicated = new ArrayList<>();
        replicated.add(raftLog.getLastIndex());
        for (String peer : peers) if (!peer.equals(nodeId)) replicated.add(matchIndex.getOrDefault(peer, 0L));
        Collections.sort(replicated);
        int majority = peers.size() / 2 + 1;
        long quorumIndex = replicated.get(replicated.size() - majority);
        if (quorumIndex > commitIndex && raftLog.getTerm(quorumIndex) == currentTerm) {
            commitIndex = quorumIndex;
            logger.info("Node {} quorum committed through index {} in term {}", nodeId, commitIndex, currentTerm);
            applyCommitted();
            sendHeartbeats(currentTerm, leadershipGeneration);
        }
    }

    private void applyCommitted() {
        if (lastApplied > commitIndex || commitIndex > raftLog.getLastIndex())
            throw new IllegalStateException("Raft application/log indices are inconsistent");
        while (lastApplied < commitIndex) {
            LogEntry entry = raftLog.getEntry(lastApplied + 1);
            if (entry == null) throw new IllegalStateException("Committed log entry missing");
            if (!entry.getNoOp()) stateMachine.apply(entry);
            lastApplied = entry.getIndex();
            logger.info("Node {} applied committed index {} entryTerm={} noOp={}", nodeId, lastApplied, entry.getTerm(), entry.getNoOp());
            CompletableFuture<SubmitCommandResponse> client = pendingCommands.remove(lastApplied);
            if (client != null) client.complete(SubmitCommandResponse.newBuilder().setCommitted(true)
                    .setTerm(currentTerm).setIndex(lastApplied).setLeaderId(nodeId).build());
        }
    }

    private void failPending(Status status) {
        for (CompletableFuture<SubmitCommandResponse> client : pendingCommands.values())
            client.completeExceptionally(status.asRuntimeException());
        pendingCommands.clear();
    }

    @Override
    public void submitCommand(SubmitCommandRequest request, StreamObserver<SubmitCommandResponse> observer) {
        Context context = Context.current();
        CompletableFuture<SubmitCommandResponse> result = new CompletableFuture<>();
        synchronized (this) {
            if (!running || !started) {
                observer.onError(Status.UNAVAILABLE.asRuntimeException());
                return;
            }
            if (state != State.LEADER) {
                result.complete(SubmitCommandResponse.newBuilder().setTerm(currentTerm).setLeaderId(leaderId).build());
            } else if (request.getData().size() > MAX_COMMAND_BYTES) {
                result.completeExceptionally(Status.INVALID_ARGUMENT.withDescription("Command exceeds 32 KiB").asRuntimeException());
            } else {
                try {
                    long index = raftLog.getLastIndex() + 1;
                    raftLog.append(LogEntry.newBuilder().setIndex(index).setTerm(currentTerm).setData(request.getData()).build());
                    pendingCommands.put(index, result);
                    advanceLeaderCommit();
                    sendHeartbeats(currentTerm, leadershipGeneration);
                } catch (IOException e) {
                    persistenceFailed(e);
                    result.completeExceptionally(Status.INTERNAL.withCause(e).asRuntimeException());
                }
            }
        }
        Context.CancellationListener cancellation = ignored -> {
            synchronized (RaftNode.this) { pendingCommands.values().removeIf(client -> client == result); }
            result.cancel(false);
        };
        context.addListener(cancellation, Runnable::run);
        result.whenComplete((response, failure) -> context.removeListener(cancellation));
        // gRPC responses run outside the state/WAL lock; never wait for a quorum on an RPC thread.
        result.whenCompleteAsync((response, failure) -> {
            if (context.isCancelled()) return;
            if (failure != null) observer.onError(failure);
            else { observer.onNext(response); observer.onCompleted(); }
        });
    }

    public record NodeStatus(String role, long term, String leaderId, long lastLogIndex,
                             long commitIndex, long lastApplied, Map<String, Long> nextIndex,
                             Map<String, Long> matchIndex) {}

    public synchronized NodeStatus status() {
        return new NodeStatus(state.name(), currentTerm, leaderId, raftLog.getLastIndex(), commitIndex,
                lastApplied, Map.copyOf(nextIndex), Map.copyOf(matchIndex));
    }

    public synchronized List<LogEntry> appliedCommands() { return stateMachine.commands(); }
    public synchronized List<LogEntry> logEntries() { return raftLog.getEntriesSince(0); }
    public synchronized int getPort() { return server == null ? port : server.getPort(); }

    @Override
    public synchronized void requestVote(
            RequestVoteRequest request,
            StreamObserver<RequestVoteResponse> responseObserver
    ) {
        if (!running) {
            responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
            return;
        }
        try {
        long requestTerm = request.getTerm();

        if (requestTerm > currentTerm) {

            becomeFollower(requestTerm);
        }

        boolean voteGranted = false;

        boolean fresh = request.getLastLogTerm() > raftLog.getLastTerm()
                || (request.getLastLogTerm() == raftLog.getLastTerm()
                && request.getLastLogIndex() >= raftLog.getLastIndex());
        if (!request.getCandidateId().isBlank() && request.getLastLogIndex() >= 0
                && request.getLastLogTerm() >= 0 && fresh && requestTerm == currentTerm &&
                (votedFor == null ||
                        votedFor.equals(request.getCandidateId()))) {

            persist(currentTerm, request.getCandidateId());
            voteGranted = true;
            resetElectionTimeout();

            logger.info(
                    "Node {} voted for {} in term {}",
                    nodeId,
                    request.getCandidateId(),
                    currentTerm
            );
        }

        RequestVoteResponse response =
                RequestVoteResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setVoteGranted(voteGranted)
                        .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        } catch (IOException e) {
            persistenceFailed(e);
            responseObserver.onError(Status.INTERNAL.withDescription("Persistent Raft state failed")
                    .withCause(e).asRuntimeException());
        }
    }

    @Override
    public synchronized void appendEntries(
            AppendEntriesRequest request,
            StreamObserver<AppendEntriesResponse> responseObserver
    ) {
        if (!running) {
            responseObserver.onError(Status.UNAVAILABLE.asRuntimeException());
            return;
        }
        try {
        boolean success = false;

        if (request.getTerm() >= currentTerm) {

            becomeFollower(request.getTerm());
            if (request.getLeaderId().isBlank() || request.getTerm() < 0 || request.getPrevLogIndex() < 0
                    || request.getPrevLogTerm() < 0 || request.getLeaderCommit() < 0)
                throw new IllegalArgumentException("Invalid AppendEntries metadata");
            leaderId = request.getLeaderId();
            resetElectionTimeout();
            validateIncomingEntries(request);
            long previous = request.getPrevLogIndex();
            boolean matches = previous == 0 ? request.getPrevLogTerm() == 0
                    : previous > 0 && raftLog.getEntry(previous) != null
                    && raftLog.getEntry(previous).getTerm() == request.getPrevLogTerm();
            if (matches && reconcileEntries(request.getEntriesList())) {
                long matchedThrough = previous + request.getEntriesCount();
                // Never commit an unmatched local suffix on the strength of an empty heartbeat.
                long leaderCommit = Math.min(request.getLeaderCommit(), matchedThrough);
                if (leaderCommit > commitIndex) {
                    commitIndex = leaderCommit;
                    applyCommitted();
                }
                success = true;
            }
        }

        AppendEntriesResponse response =
                AppendEntriesResponse.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(success)
                        .setLastLogIndex(raftLog.getLastIndex())
                        .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
        } catch (IOException e) {
            persistenceFailed(e);
            responseObserver.onError(Status.INTERNAL.withDescription("Persistent Raft state failed")
                    .withCause(e).asRuntimeException());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private void validateIncomingEntries(AppendEntriesRequest request) {
        long index = request.getPrevLogIndex();
        long term = request.getPrevLogTerm();
        for (LogEntry entry : request.getEntriesList()) {
            if (index == Long.MAX_VALUE || entry.getIndex() != ++index || entry.getTerm() < term
                    || entry.getTerm() > request.getTerm() || (entry.getNoOp() && !entry.getData().isEmpty()))
                throw new IllegalArgumentException("Invalid AppendEntries log sequence");
            term = entry.getTerm();
        }
    }

    private boolean reconcileEntries(List<LogEntry> entries) throws IOException {
        long conflict = 0;
        for (LogEntry entry : entries) {
            LogEntry existing = raftLog.getEntry(entry.getIndex());
            if (existing == null) break;
            if (existing.getTerm() != entry.getTerm()) {
                if (entry.getIndex() <= commitIndex) {
                    logger.error("Node {} refused a conflict at committed index {}", nodeId, entry.getIndex());
                    return false;
                }
                conflict = entry.getIndex();
                break;
            }
            if (!existing.getData().equals(entry.getData()) || existing.getNoOp() != entry.getNoOp())
                throw new IllegalArgumentException("Same index/term has different command data");
        }
        if (conflict != 0) raftLog.truncateAt(conflict);
        for (LogEntry entry : entries) if (entry.getIndex() > raftLog.getLastIndex()) raftLog.append(entry);
        return true;
    }

    public void blockUntilShutdown()
            throws InterruptedException {

        if (server != null) {
            server.awaitTermination();
        }
    }

    public synchronized void stop() {

        if (!running) {
            return;
        }

        running = false;
        // Orderly stop/recreate must not retain every old node through JVM shutdown hooks.
        if (shutdownHook != null) {
            try { Runtime.getRuntime().removeShutdownHook(shutdownHook); }
            catch (IllegalStateException ignored) { /* JVM shutdown is already in progress. */ }
            shutdownHook = null;
        }
        state = State.FOLLOWER;
        failPending(Status.UNAVAILABLE.withDescription("Node stopped; command outcome is unknown"));
        cancelHeartbeats();
        if (electionTask != null) electionTask.cancel(true);

        logger.info("Stopping node {}", nodeId);

        scheduler.shutdownNow();

        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }

        if (server != null) {
            server.shutdownNow();
        }
        try { raftLog.close(); }
        catch (IOException e) { logger.error("Closing WAL failed", e); }
        try { persistentState.close(); }
        catch (IOException e) { logger.error("Closing hard state failed", e); }
    }
}
