# Raft Consensus Engine

A Java 17-targeted Raft implementation with gRPC/Protobuf, per-node WALs,
persistent term/vote, log replication, conflict reconciliation, quorum commit,
and ordered committed-command application.

Explore consensus across independent Java processes: elect a leader, commit a
command through a majority, kill the leader, and watch a recovered node catch up.

## What This Project Demonstrates

- Three-node clusters using real gRPC communication and Docker Compose.
- Log-aware voting, persistent `currentTerm`/`votedFor`, and election safety tests.
- Log replication, conflicting-suffix reconciliation, and `nextIndex`/`matchIndex` tracking.
- Current-term quorum commitment and ordered committed-entry application.
- WAL-backed persistence, leader failover, process/container restart recovery,
  and follower catch-up.
- 56 passing automated tests, including real-node and separate-process integration tests.

These capabilities were exercised in the scenarios below; they are not a formal
correctness proof or an exhaustive fault-tolerance guarantee.

## Quick Start

Requirements: Docker Desktop/Engine running **Linux containers**, Docker Compose,
internet access for the initial image/dependency downloads, and free host ports
50051–50053. No host Java installation is required for the Docker demo.

```sh
git clone https://github.com/thekarthikh/raft-consensus-engine.git
cd raft-consensus-engine
docker compose up --build
```

Keep that terminal open. In a second terminal, from the same directory:

```sh
docker compose ps
docker compose logs --tail 40
```

Expect three running services and a message such as
`Node node1 BECAME LEADER for term 1`. Any node can win; do not assume node1.
Startup and reconnect can take several seconds.

If an already-open terminal cannot find Docker after installation, open a fresh
terminal and confirm `docker version` and `docker compose version` work.

## 3-Node Demonstration

The following variable-based commands use PowerShell. The small leader helper
selects the highest-term leadership message from currently running services.
It reads logs, **not a live leadership RPC**: wait for a stable election first.
If scripts are blocked by local policy, set `$leader = 'node1'` manually using
the actual leader ID shown in the logs. On other shells, use literal node IDs
in place of the PowerShell variables; an example is given below.

### 1. Find the leader and submit a command

```powershell
$leader = ./scripts/leader.ps1
$leader
docker compose exec -T $leader java -cp /app/app.jar com.raft.RaftClientApp $leader 50051 first
docker compose logs --tail 20
```

For a node1 leader, the shell-independent write command is:

```sh
docker compose exec -T node1 java -cp /app/app.jar com.raft.RaftClientApp node1 50051 first
```

Run only the appropriate write command, not both examples.
The client prints `COMMITTED leader=... term=... index=...` after majority
commitment and leader application. Find that index in the leader's
`quorum committed` message and each follower's `applied committed index`
message. Election no-ops also occupy indexes.

A direct follower write fails and reports its known leader ID. The client
does not automatically redirect or retry. A timeout means **unknown outcome**:
the original entry may commit later, so blindly retrying can create duplicates.

### 2. Force-kill the leader and write through its replacement

```powershell
$oldLeader = $leader
$leaderContainer = docker compose ps -q $oldLeader
docker kill $leaderContainer
docker compose ps --all
docker compose logs --tail 30
```

Wait for a newer-term `BECAME LEADER` message from a surviving service, then:

```powershell
$leader = ./scripts/leader.ps1
$leader
docker compose exec -T $leader java -cp /app/app.jar com.raft.RaftClientApp $leader 50051 second
docker compose logs --tail 20
```

Replication errors naming the stopped peer are expected; the other two nodes
can still commit. Do not treat historical leader messages as current during
a transition.

### 3. Recover the old leader and verify convergence

```powershell
docker compose start $oldLeader
docker compose logs --tail 30 $oldLeader
```

Look for `Recovered ... entries`, then ordered application of the missing
committed indexes. Docker DNS/gRPC reconnect delay can cause additional
elections before catch-up; allow time for the cluster to settle.

Once stable, compare the WAL bytes:

```sh
docker compose exec -T node1 sha256sum /app/data/node1/raft_log_node1.bin
docker compose exec -T node2 sha256sum /app/data/node2/raft_log_node2.bin
docker compose exec -T node3 sha256sum /app/data/node3/raft_log_node3.bin
```

Hashes should converge when replication is caught up and no new entries are
being appended. Also check each node's application messages; hashes alone
do not prove commitment/application.

An additional restart can be demonstrated with:

```sh
docker compose restart node3
docker compose logs --tail 30 node3
```

Node3 might be leader at that moment, so another election may occur.

### 4. Stop without erasing history

```sh
docker compose down
```

Named volumes remain. A later startup recovers them. Keep the same Compose
project name/directory between runs: changing it selects different volumes.
Do not delete volumes during recovery testing. Repeated demonstrations append
additional commands; there is no deduplication.

## Architecture

```text
                       Client: SubmitCommand
                                |
                         Leader + own WAL
                          /            \
                 AppendEntries      AppendEntries
                       v                  v
                Follower + WAL     Follower + WAL
                          \            /
                         majority ACK
                                |
                    current-term quorum commit
                                |
                 ordered in-memory application
                                |
          leaderCommit propagates application to followers
```

Each node has separate storage. RequestVote advertises real log metadata;
leaders track nextIndex/matchIndex and reconcile conflicting suffixes.
A leadership no-op permits commitment of an earlier-term replicated prefix.
The state machine is an ordered in-memory opaque-command history, not a
database or a read-consistency API. After restart, it is rebuilt once the
cluster reestablishes the committed prefix.

## Raft Flow

1. An election uses persisted terms/votes and log freshness to select a leader.
2. The leader durably appends a client command before sending AppendEntries.
3. Followers validate the preceding index/term and reconcile only conflicting suffixes.
4. Successful responses advance follower `matchIndex` and `nextIndex`; inconsistencies backtrack.
5. A majority-replicated current-term entry advances `commitIndex`.
6. Nodes apply committed entries in order and advance `lastApplied`; followers learn the commit through AppendEntries.
7. After failure, surviving nodes elect a replacement. A restarted node recovers its storage and catches up.

## Persistence

Each node owns a length-prefixed Protobuf WAL and persistent Raft hard state.
Append forces the file before publishing new in-memory log metadata. Recovery
preserves complete records and truncates an incomplete final header/payload;
complete invalid records fail closed. Hard-state replacement forces a temporary
file before atomic rename and attempts to force the parent directory.

Docker gives every node a separate named volume. Restart tests reconstruct the
log and persisted term/vote, then rebuild ordered application after the cluster
reestablishes commitment. Process/container restart evidence is distinct from
hardware power-loss testing; directory forcing was unavailable on Windows.

## Testing and Java Fallback

Requirements: Maven 3.9+ and an available JDK 17+. Maven targets Java 17.
Docker validation used Temurin 17; host validation used Corretto 22.0.2.

```sh
mvn clean test
mvn package
git diff --check
mvn test -Dtest=RaftMultiProcessIntegrationTest -Draft.processJar=target/raft-engine-1.0-SNAPSHOT.jar
```

The final validated suite passed **56/56 tests**, including 10 real-node gRPC integration tests,
two separate-process integration tests, WAL recovery/invariant tests, persistent
term/vote tests, and real-gRPC CLI tests. The packaged-JAR process test starts
three child JVMs, submits commands, forcibly kills the leader, verifies failover,
restarts nodes, and checks identical logs and ordered applied history.
It cleans up its processes; temporary evidence is under `target/raft-process*`.

For a manual Java cluster, run these in three terminals:

```sh
java -Draft.dataDir=data -jar target/raft-engine-1.0-SNAPSHOT.jar node1 50051 node1=127.0.0.1:50051,node2=127.0.0.1:50052,node3=127.0.0.1:50053
java -Draft.dataDir=data -jar target/raft-engine-1.0-SNAPSHOT.jar node2 50052 node1=127.0.0.1:50051,node2=127.0.0.1:50052,node3=127.0.0.1:50053
java -Draft.dataDir=data -jar target/raft-engine-1.0-SNAPSHOT.jar node3 50053 node1=127.0.0.1:50051,node2=127.0.0.1:50052,node3=127.0.0.1:50053
```

Submit to the actual leader's host port, for example:

```sh
java -cp target/raft-engine-1.0-SNAPSHOT.jar com.raft.RaftClientApp 127.0.0.1 50051 first
```

Stop each JVM with Ctrl+C. This is orderly shutdown, not forced-crash testing.
Do not run the Java cluster and Docker cluster on the same host ports together.
Ensure the launching `java` is 17+; Maven toolchain selection does not change PATH.

## Benchmark: local WAL, not distributed Raft

From PowerShell, with Java 17+ on PATH:

```powershell
./scripts/benchmark.ps1
```

The opt-in JMH 1.37 profile builds
`com.raft.benchmark.RaftLogBenchmark.testLogAppend`. It measures local entry
construction, Protobuf serialization, FileChannel writing, **force(true) for
every entry**, and in-memory metadata publication. It does not measure gRPC,
replication, quorum-commit throughput, or end-to-end distributed writes.

Measured on Windows NTFS / Corretto 22.0.2: **1198.087 ± 233.214 local
appends/second**, JMH-reported 99.9% confidence interval. Parameters: 128-byte
payload, one thread, one fork, two 1-second warmups, five 2-second measurements,
`-Xms256m -Xmx256m`. Each iteration uses a fresh default-temporary-directory
WAL; setup and cleanup are outside measurement.

This short, environment-specific run is not a portable throughput guarantee.
[Recorded result](benchmarks/results/wal-windows-corretto22.json) retains scores,
raw samples, and JVM metadata; the machine-local executable path was removed.
The script writes fresh raw output under `target/wal-benchmark.json`.
Distributed performance claim not verified; no distributed writes/sec number
is claimed.

## Technology Stack

Java 17 target · gRPC · Protocol Buffers · Maven · Docker / Docker Compose ·
JUnit 5 · JMH 1.37. Docker runs Java 17; a newer host JDK can compile with release 17.

## Project Structure

```text
src/main/java/com/raft/
  RaftNodeApp.java / RaftClientApp.java   Node and command entry points
  core/                                 Elections, replication, application
  storage/                              WAL and persistent hard state
src/main/proto/raft.proto                RPC messages and services
src/test/                               Unit, gRPC, process, and JMH tests
scripts/                                Leader helper and benchmark runner
benchmarks/results/                     Recorded local WAL benchmark
Dockerfile / docker-compose.yml          Three-node deployment
pom.xml                                 Java 17 build and optional JMH profile
```

## Design Decisions

- Persist term/vote before responses to preserve election safety across restarts.
- Use one authoritative WAL per node, with bounded records and explicit suffix truncation.
- Track per-follower replication progress instead of treating heartbeats as replication.
- Commit via a majority and the current-term rule; apply only the committed prefix.
- Separate node volumes and test real child JVMs to exercise process boundaries.
- Keep the state machine small: ordered opaque commands, without invented database semantics.

## Configuration

| Node | Host gRPC port | Internal peer | Persistent volume / directory |
|---|---|---|---|
| node1 | 50051 | node1:50051 | node1-data / /app/data/node1 |
| node2 | 50052 | node2:50051 | node2-data / /app/data/node2 |
| node3 | 50053 | node3:50051 | node3-data / /app/data/node3 |

Compose supplies fixed node IDs, static peer addresses, and separate named
volumes mounted at `/app/data`. The Dockerfile copies the exact shaded JAR.
The image build skips tests; run the Maven suite separately for validation.
No Windows-specific path is required by the Docker configuration.

JVM properties, set before `-jar`:

| Setting | Default |
|---|---|
| raft.dataDir | data; each node uses its ID subdirectory |
| raft.electionMinMillis | 1500 |
| raft.electionMaxMillis | 3000, exclusive upper bound |
| raft.heartbeatMillis | 500 |

Election minimum must be at least three heartbeat intervals; maximum must
exceed minimum. Client payloads are limited to 32 KiB; serialized WAL records
to 1 MiB. Keep identities, directories, and membership stable. Storage locking
prevents concurrent ownership of the same node directory. Legacy WAL layouts
require explicit migration rather than silent adoption.

## Deployment

The single-host Compose deployment is the primary verified local demo.
For a VPS, Docker and these persistent volumes/ports are the intended layout,
but no VPS/public deployment has been performed. Ports bind on host interfaces:
gRPC is plaintext and unauthenticated, so restrict them to trusted/private
networks. Do not expose this as a secure public write service.
For separate servers, use stable IDs/storage and a common explicit membership
map of reachable private host:port addresses; Docker service DNS does not span
servers. That topology is not verified.

## Failure Demonstration

Verified in exercised scenarios: three Docker containers and three separate
Java processes, election, real gRPC replication, conflict reconciliation,
majority/current-term commit, ordered application, leader failover, follower
catch-up, WAL restart recovery, and persistent term/vote.

The Docker demonstration committed `first` at term 1/index 2, killed node1,
committed `second` through node2 at term 2/index 4, restarted node1, and observed
ordered catch-up and identical WAL hashes. Further container restart also
recovered history. Named volumes were retained throughout.

## Limitations

Not implemented: application database semantics, client deduplication,
linearizable reads, snapshots/log compaction, dynamic membership, TLS/auth,
and WAL checksums. Log and application history grow without compaction.

WAL recovery accumulates short reads, bounds lengths, preserves complete
preceding records, and truncates incomplete EOF tails. Complete invalid records
fail closed. Deterministic torn-record fixtures are not hardware power-loss
tests. Hard-state files are forced before atomic replacement; directory force
is attempted. Windows directory forcing was unavailable. Successful Linux
container restart is not proof of power-loss or every rename-crash-point safety.

Not verified: hardware power loss, exhaustive network partitions, every
storage crash point, complete-cluster OS crash, distributed throughput,
multi-server hosting, or formal Raft correctness.

Client deduplication/exactly-once semantics and a linearizable read API are not
provided. gRPC is plaintext and unauthenticated. WAL records have no checksums,
so corruption detection is incomplete. No public/VPS deployment has been performed.
An intermittent Windows `AccessDeniedException` during hard-state atomic
replacement was also observed in host validation. The node fails closed rather
than acknowledging an unpersisted vote; the external cause was not established.

## Interviewer Quick Demo

1. Clone and run `docker compose up --build`.
2. In another terminal, run `docker compose ps` and inspect election logs.
3. Use the [3-node demonstration](#3-node-demonstration) to submit a command.
4. Find its commit index in the leader and follower application logs.
5. Kill the detected leader and submit through the newly elected leader.
6. Restart the old leader; inspect recovery, application, and converged WAL hashes.
7. Run `docker compose down` to stop while retaining persistent volumes.
