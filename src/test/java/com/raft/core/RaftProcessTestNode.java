package com.raft.core;

import com.raft.rpc.LogEntry;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Test-only launcher: real RaftNode/gRPC/WAL, with stdout observations, no new production RPC. */
public final class RaftProcessTestNode {
    public static void main(String[] args) throws Exception {
        RaftNode node = new RaftNode(args[0], Integer.parseInt(args[1]), Arrays.asList(args[2].split(",")));
        // Observe reconstruction before any election or peer traffic can repair the log.
        System.out.println("RECOVERED\t" + node.status().term() + "\t" + encode(node.logEntries()));
        System.out.flush();
        node.start();
        AtomicBoolean observing = new AtomicBoolean(true);
        Thread control = new Thread(() -> {
            try {
                new BufferedReader(new InputStreamReader(System.in)).readLine();
                node.stop();
                node.blockUntilShutdown();
                observing.set(false);
            } catch (Exception e) { e.printStackTrace(); System.exit(2); }
        }, "test-process-control");
        control.setDaemon(true);
        control.start();
        while (observing.get()) {
            synchronized (node) {
                RaftNode.NodeStatus s = node.status();
                System.out.println("SNAPSHOT\t" + s.role() + "\t" + s.term() + "\t" + s.commitIndex()
                        + "\t" + s.lastApplied() + "\t" + s.lastLogIndex() + "\t"
                        + encode(node.logEntries()) + "\t" + encode(node.appliedCommands()));
                System.out.flush();
            }
            Thread.sleep(100);
        }
    }

    private static String encode(List<LogEntry> entries) {
        return entries.stream().map(e -> Base64.getEncoder().encodeToString(e.toByteArray()))
                .collect(Collectors.joining(","));
    }
}
