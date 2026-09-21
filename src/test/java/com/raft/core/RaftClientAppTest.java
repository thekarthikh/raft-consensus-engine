package com.raft.core;

import com.raft.RaftClientApp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class RaftClientAppTest {
    @TempDir Path root;
    @Test void clientMainCommitsOnRealLeader() throws Exception {
        RaftNode node = new RaftNode("node1", 0, List.of("node1"), root, 300, 600, 50);
        try {
            node.start();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while (!node.status().role().equals("LEADER") && System.nanoTime() < deadline) Thread.sleep(20);
            assertEquals("LEADER", node.status().role());
            RaftClientApp.main(new String[]{"127.0.0.1", Integer.toString(node.getPort()), "cli-command"});
            assertEquals(List.of("cli-command"), node.appliedCommands().stream().map(e -> e.getData().toStringUtf8()).toList());
            assertEquals(2, node.status().commitIndex());
        } finally { node.stop(); node.blockUntilShutdown(); }
    }
    @Test void clientMainRejectsRealFollowerWithoutAppending() throws Exception {
        RaftNode node = new RaftNode("node1", 0, List.of("node1"), root, 60000, 90000, 500);
        try {
            node.start();
            assertThrows(IllegalStateException.class, () -> RaftClientApp.main(new String[]{"127.0.0.1", Integer.toString(node.getPort()), "reject"}));
            assertEquals(0, node.status().lastLogIndex());
        } finally { node.stop(); node.blockUntilShutdown(); }
    }
}
