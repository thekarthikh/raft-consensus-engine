package com.raft.core;

import com.raft.rpc.LogEntry;
import java.util.ArrayList;
import java.util.List;

/** Minimal demonstration state: committed opaque commands, not a database or read-consistency API.
 * It is recreated empty on restart; Raft must establish the committed prefix before replaying it.
 */
final class CommittedStateMachine {
    private final List<LogEntry> commands = new ArrayList<>();
    private long lastCommandIndex;

    void apply(LogEntry entry) {
        if (entry.getIndex() <= lastCommandIndex || entry.getNoOp())
            throw new IllegalStateException("Commands must be applied once in log order");
        commands.add(entry);
        lastCommandIndex = entry.getIndex();
    }

    List<LogEntry> commands() { return List.copyOf(commands); }
}
