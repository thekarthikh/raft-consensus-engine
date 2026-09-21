package com.raft.core;

import com.google.protobuf.ByteString;
import com.raft.rpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.File;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class RaftMultiProcessIntegrationTest {
    @Test void threeSeparateProcessesCommitFailOverAndRecoverKilledLeader() throws Exception {
        try (Cluster c = new Cluster(false)) {
            // Establish the requested initial leader before starting the third JVM.
            // JVM launch/connection latency can otherwise let node3 legitimately win,
            // even though node1 has a shorter election timeout.
            c.start(0);
            c.await("node1 server observed before launching peers", () -> c.snapshot(0) != null);
            c.start(1);
            c.await("node1 elected with a real two-of-three quorum", () -> c.leader() == 0 && c.snapshot(1) != null);
            c.start(2);
            c.await("node1 elected; all three processes live", () -> c.leader() == 0 && c.allObserved());
            assertEquals(3, Arrays.stream(c.processes).map(Process::pid).distinct().count());
            long first = c.submit(0, "before-crash");
            c.await("first quorum commit applied everywhere", () -> c.allApplied(List.of("before-crash"), first));
            long oldTerm = c.snapshot(0).term;
            List<LogEntry> beforeCrashLog = c.snapshot(0).log;
            System.out.printf("ACTUAL MULTI-PROCESS: pids=%s ports=%s node1 leader term=%d first commit=%d logs=%s%n",
                    Arrays.toString(Arrays.stream(c.processes).mapToLong(Process::pid).toArray()),
                    Arrays.toString(c.ports), oldTerm, first, c.root);

            c.kill(0); // destroyForcibly: no normal close, no node shutdown hook.
            c.await("remaining processes elect newer-term leader", () -> {
                int leader = c.leader();
                return leader > 0 && c.snapshot(leader).term > oldTerm;
            });
            int newLeader = c.leader();
            long second = c.submit(newLeader, "after-crash");
            c.await("survivors commit/apply after failover", () -> c.applied(1, List.of("before-crash", "after-crash"), second)
                    && c.applied(2, List.of("before-crash", "after-crash"), second));

            Path crashedWal = c.root.resolve("store-node1/node1/raft_log_node1.bin");
            long validBytes = Files.size(crashedWal);
            // Deterministic torn-tail fixture after a real forced process kill. This is not
            // represented as a naturally observed interrupted FileChannel.write.
            Files.write(crashedWal, new byte[]{0, 0}, StandardOpenOption.APPEND);
            c.start(0);
            c.await("startup recovery observation available", () -> c.recoveredLog(0) != null);
            assertEquals(beforeCrashLog, c.recoveredLog(0), "WAL reconstructed before rejoining, not fetched from peers");
            c.await("killed leader WAL recovered and caught up", () -> c.allApplied(List.of("before-crash", "after-crash"), second));
            List<LogEntry> leaderLog = c.snapshot(newLeader).log;
            assertEquals(leaderLog, c.snapshot(0).log);
            assertTrue(c.snapshot(0).term >= oldTerm);
            assertTrue(Files.size(crashedWal) > validBytes, "new leader entries appended after torn-tail repair");

            // Also exercise an orderly process restart and catch-up while it was down.
            // Recovery can trigger another election; do not reuse the pre-rejoin leader.
            c.await("all live processes agree on a leader after rejoin", () -> c.stableLeader() >= 0);
            int restartLeader = c.stableLeader();
            assertTrue(restartLeader >= 0, "agreed leader before stopping a follower");
            int follower = (restartLeader + 1) % 3;
            List<LogEntry> beforeOrderlyRestart = c.snapshot(follower).log;
            c.stopGracefully(follower);
            long third = c.submit(restartLeader, "during-restart");
            c.start(follower);
            c.await("orderly restart WAL observation available", () -> c.recoveredLog(follower) != null);
            assertEquals(beforeOrderlyRestart, c.recoveredLog(follower));
            c.await("orderly restarted follower recovers and applies in order", () ->
                    c.allApplied(List.of("before-crash", "after-crash", "during-restart"), third));
            for (int i = 0; i < 3; i++) {
                assertEquals(c.snapshot(newLeader).log, c.snapshot(i).log);
                assertTrue(c.snapshot(i).applied <= c.snapshot(i).commit);
            }
            System.out.printf("ACTUAL MULTI-PROCESS: killed leader exit=%d; new leader=node%d; "
                    + "commits=%d,%d,%d; all three identical logs and ordered applied commands; recovered WALs under %s%n",
                    c.killedExit, newLeader + 1, first, second, third, c.root);
        }
    }

    @Test void forcedProcessRestartRetainsTermAndVoteAndCannotVoteTwice() throws Exception {
        try (Cluster c = new Cluster(true)) {
            c.start(0);
            c.await("vote-test process starts", () -> c.snapshot(0) != null);
            assertTrue(c.vote(42, "node2").getVoteGranted());
            c.await("term 42 observed", () -> c.snapshot(0).term == 42);
            c.kill(0);
            c.start(0);
            c.await("persisted term reconstructed on process restart", () -> c.snapshot(0) != null && c.snapshot(0).term == 42);
            RequestVoteResponse secondCandidate = c.vote(42, "node3");
            assertEquals(42, secondCandidate.getTerm());
            assertFalse(secondCandidate.getVoteGranted());
            assertTrue(c.vote(42, "node2").getVoteGranted());
            RequestVoteResponse stale = c.vote(41, "node3");
            assertFalse(stale.getVoteGranted()); assertEquals(42, stale.getTerm());
            System.out.printf("ACTUAL HARD-STATE PROCESS CRASH: forced exit=%d, restart term=42, "
                    + "persisted node2 vote retained, node3 same-term/stale votes rejected; files=%s%n", c.killedExit, c.root);
        }
    }

    private record Snapshot(String role, long term, long commit, long applied, long lastIndex,
                            List<LogEntry> log, List<LogEntry> commands) {}

    private static final class Cluster implements AutoCloseable {
        final Path root = Files.createTempDirectory(Path.of("target"), "raft-process-").toAbsolutePath();
        final int[] ports = new int[3];
        final Process[] processes = new Process[3];
        final Path[] outputs = new Path[3];
        final ManagedChannel[] channels = new ManagedChannel[3];
        final boolean slowElections;
        int generation;
        int killedExit;

        Cluster(boolean slowElections) throws IOException {
            this.slowElections = slowElections;
            List<ServerSocket> reservations = new ArrayList<>();
            try {
                for (int i = 0; i < 3; i++) {
                    ServerSocket s = new ServerSocket(0); reservations.add(s); ports[i] = s.getLocalPort();
                    channels[i] = ManagedChannelBuilder.forAddress("127.0.0.1", ports[i]).usePlaintext().build();
                }
            } finally { for (ServerSocket s : reservations) s.close(); }
        }

        void start(int i) throws IOException {
            assertTrue(processes[i] == null || !processes[i].isAlive());
            outputs[i] = root.resolve("node" + (i + 1) + "-run" + (++generation) + ".log");
            String members = "node1=127.0.0.1:" + ports[0] + ",node2=127.0.0.1:" + ports[1]
                    + ",node3=127.0.0.1:" + ports[2];
            String java = Path.of(System.getProperty("java.home"), "bin", "java.exe").toString();
            if (!Files.exists(Path.of(java))) java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            long min = slowElections ? 60000 : i == 0 ? 1200 : 3000;
            long max = slowElections ? 90000 : i == 0 ? 2000 : 5000;
            String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            String packagedJar = System.getProperty("raft.processJar");
            if (packagedJar != null) {
                assertTrue(Files.isRegularFile(Path.of(packagedJar)), "packaged JAR must already exist");
                // Only the observer launcher comes from test-classes; every production class
                // and runtime dependency comes from the actual distributable JAR.
                classpath = Path.of("target/test-classes").toAbsolutePath() + File.pathSeparator
                        + Path.of(packagedJar).toAbsolutePath();
            }
            List<String> command = List.of(java, "-Draft.dataDir=" + root.resolve("store-node" + (i + 1)),
                    "-Draft.electionMinMillis=" + min, "-Draft.electionMaxMillis=" + max, "-Draft.heartbeatMillis=200",
                    "-cp", classpath,
                    RaftProcessTestNode.class.getName(), "node" + (i + 1), Integer.toString(ports[i]), members);
            Path recordedCommand = outputs[i].resolveSibling(outputs[i].getFileName() + ".command.txt");
            Files.write(recordedCommand, command.stream().map(arg -> "\"" + arg + "\"").toList());
            System.out.printf("CHILD START: node%d port=%d data=%s exact argv=%s%n", i + 1, ports[i],
                    root.resolve("store-node" + (i + 1)), recordedCommand);
            processes[i] = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(outputs[i].toFile()).start();
        }

        Snapshot snapshot(int i) {
            if (i < 0 || outputs[i] == null || processes[i] == null || !processes[i].isAlive()) return null;
            try {
                List<String> lines = Files.readAllLines(outputs[i]);
                for (int n = lines.size() - 1; n >= 0; n--) {
                    if (!lines.get(n).startsWith("SNAPSHOT\t")) continue;
                    String[] p = lines.get(n).split("\t", -1);
                    if (p.length != 8) continue; // A currently-being-written line is not a snapshot.
                    return new Snapshot(p[1], Long.parseLong(p[2]), Long.parseLong(p[3]), Long.parseLong(p[4]),
                            Long.parseLong(p[5]), decode(p[6]), decode(p[7]));
                }
            } catch (IOException | IllegalArgumentException ignored) { /* polling an incomplete observation */ }
            return null;
        }

        private List<LogEntry> decode(String text) throws IOException {
            List<LogEntry> entries = new ArrayList<>();
            if (!text.isEmpty()) for (String encoded : text.split(",")) entries.add(LogEntry.parseFrom(Base64.getDecoder().decode(encoded)));
            return List.copyOf(entries);
        }

        List<LogEntry> recoveredLog(int i) {
            try {
                for (String line : Files.readAllLines(outputs[i])) {
                    if (line.startsWith("RECOVERED\t")) {
                        String[] p = line.split("\t", -1);
                        if (p.length == 3) return decode(p[2]);
                    }
                }
            } catch (IOException | IllegalArgumentException ignored) { /* incomplete stdout observation */ }
            return null;
        }

        int leader() {
            int leader = -1;
            for (int i = 0; i < 3; i++) {
                Snapshot s = snapshot(i);
                if (s != null && s.role.equals("LEADER")) { if (leader != -1) return -1; leader = i; }
            }
            return leader;
        }

        boolean allObserved() { for (int i = 0; i < 3; i++) if (snapshot(i) == null) return false; return true; }

        int stableLeader() {
            int leader = leader();
            if (leader < 0) return -1;
            Snapshot elected = snapshot(leader);
            if (elected == null || !elected.role.equals("LEADER")) return -1;
            for (int i = 0; i < 3; i++) {
                Snapshot member = snapshot(i);
                if (member == null || member.term != elected.term
                        || (i != leader && !member.role.equals("FOLLOWER"))) return -1;
            }
            return leader;
        }

        boolean applied(int i, List<String> expected, long index) {
            Snapshot s = snapshot(i);
            return s != null && s.commit >= index && s.applied >= index && s.lastIndex >= index
                    && s.commands.stream().map(e -> e.getData().toStringUtf8()).toList().equals(expected);
        }

        boolean allApplied(List<String> expected, long index) {
            for (int i = 0; i < 3; i++) if (!applied(i, expected, index)) return false;
            return true;
        }

        long submit(int i, String data) {
            SubmitCommandResponse response = RaftServiceGrpc.newBlockingStub(channels[i]).withDeadlineAfter(10, TimeUnit.SECONDS)
                    .submitCommand(SubmitCommandRequest.newBuilder().setData(ByteString.copyFromUtf8(data)).build());
            assertTrue(response.getCommitted()); assertEquals("node" + (i + 1), response.getLeaderId());
            return response.getIndex();
        }

        RequestVoteResponse vote(long term, String candidate) {
            return RaftServiceGrpc.newBlockingStub(channels[0]).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .requestVote(RequestVoteRequest.newBuilder().setTerm(term).setCandidateId(candidate).build());
        }

        void kill(int i) throws Exception {
            assertTrue(processes[i].isAlive());
            processes[i].destroyForcibly();
            assertTrue(processes[i].waitFor(15, TimeUnit.SECONDS), "forced process exit");
            killedExit = processes[i].exitValue();
            System.out.printf("FORCED PROCESS KILL: node%d pid=%d exit=%d (no orderly close)%n", i + 1, processes[i].pid(), killedExit);
        }

        void stopGracefully(int i) throws Exception {
            if (processes[i] == null || !processes[i].isAlive()) return;
            processes[i].getOutputStream().write("stop\n".getBytes(StandardCharsets.UTF_8));
            processes[i].getOutputStream().flush();
            assertTrue(processes[i].waitFor(15, TimeUnit.SECONDS), "orderly process exit");
            assertEquals(0, processes[i].exitValue(), "orderly process exit code; logs=" + outputs[i]);
        }

        void await(String description, BooleanSupplier condition) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
            while (System.nanoTime() < deadline) {
                if (condition.getAsBoolean()) return;
                Thread.sleep(100);
            }
            fail(description + "; observations=" + Arrays.toString(new Snapshot[]{snapshot(0), snapshot(1), snapshot(2)})
                    + "; child logs=" + root);
        }

        @Override public void close() throws Exception {
            try {
                for (int i = 0; i < 3; i++) stopGracefully(i);
            } finally {
                // Even a failed shutdown assertion must not orphan the other child processes.
                try {
                    for (Process process : processes) if (process != null && process.isAlive()) {
                        process.destroyForcibly(); process.waitFor(15, TimeUnit.SECONDS);
                    }
                } finally {
                    for (ManagedChannel channel : channels) if (channel != null) {
                        channel.shutdownNow(); channel.awaitTermination(5, TimeUnit.SECONDS);
                    }
                }
            }
        }
    }
}
