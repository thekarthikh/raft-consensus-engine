package com.raft.benchmark;

import com.raft.rpc.LogEntry;
import com.raft.storage.RaftLog;
import com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Comparator;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@Threads(1)
public class RaftLogBenchmark {

    private RaftLog raftLog;
    private LogEntry sampleEntry;
    private AtomicLong index = new AtomicLong(1);
    private Path tempDir;

    @Setup(Level.Iteration)
    public void setup() throws IOException {
        tempDir = Files.createTempDirectory("raft_bench");
        index.set(1);
        raftLog = new RaftLog(tempDir.toString(), "bench");
        sampleEntry = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(1)
                .setData(ByteString.copyFrom(new byte[128])) // 128 byte payload
                .build();
    }

    @TearDown(Level.Iteration)
    public void tearDown() throws IOException {
        raftLog.close();
        // Delete only this benchmark's generated directory, after closing the WAL.
        try (var paths = Files.walk(tempDir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
    }

    @Benchmark
    public void testLogAppend() throws IOException {
        long idx = index.getAndIncrement();
        raftLog.append(LogEntry.newBuilder(sampleEntry).setIndex(idx).build());
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(RaftLogBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
