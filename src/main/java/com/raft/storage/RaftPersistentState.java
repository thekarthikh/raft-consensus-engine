package com.raft.storage;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;

/** Term/vote hard state. Log metadata is derived from the WAL, not duplicated here.
 * Atomic replacement and file forcing are a foundation, not a complete crash-recovery guarantee.
 */
public final class RaftPersistentState implements AutoCloseable {
    private static final int VERSION = 1;
    private final Path stateFile;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private long currentTerm;
    private String votedFor;

    public RaftPersistentState(Path directory) throws IOException {
        boolean newDirectory = !Files.exists(directory);
        Files.createDirectories(directory);
        if (newDirectory && directory.toAbsolutePath().getParent() != null)
            StorageDurability.forceDirectory(directory.toAbsolutePath().getParent());
        stateFile = directory.resolve("hard-state.bin");
        boolean existingStorage = Files.exists(directory.resolve("node.lock"));
        lockChannel = FileChannel.open(directory.resolve("node.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock acquired;
        try {
            acquired = lockChannel.tryLock();
            if (acquired == null) throw new IOException("Raft storage already in use: " + directory);
        } catch (IOException | RuntimeException e) {
            lockChannel.close();
            throw e;
        }
        lock = acquired;
        try {
            if (Files.exists(stateFile)) {
                try (DataInputStream input = new DataInputStream(Files.newInputStream(stateFile))) {
                    if (input.readInt() != VERSION) throw new IOException("Unsupported hard-state version");
                    currentTerm = input.readLong();
                    votedFor = input.readBoolean() ? input.readUTF() : null;
                    if (currentTerm < 0 || (votedFor != null && votedFor.isBlank()) || input.read() != -1)
                        throw new IOException("Invalid Raft hard state");
                }
            } else {
                if (existingStorage)
                    throw new IOException("Missing hard state in previously initialized storage; refusing term reset");
                if (Files.exists(stateFile.resolveSibling("hard-state.tmp")))
                    throw new IOException("Missing hard state with orphaned temporary file; refusing term reset");
                save(0, null);
            }
        } catch (IOException | RuntimeException e) {
            close();
            throw e;
        }
    }

    public synchronized void save(long term, String vote) throws IOException {
        if (term < currentTerm || term < 0 || (vote != null && vote.isBlank()))
            throw new IllegalArgumentException("Invalid term/vote transition");
        if (term == currentTerm && votedFor != null && !votedFor.equals(vote))
            throw new IllegalArgumentException("Cannot change a vote within a term");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(VERSION);
            output.writeLong(term);
            output.writeBoolean(vote != null);
            if (vote != null) output.writeUTF(vote);
        }
        Path temporary = stateFile.resolveSibling("hard-state.tmp");
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes.toByteArray());
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        // No non-atomic fallback: an unsupported atomic move must fail the node closed.
        Files.move(temporary, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        // Persist the replacement directory entry where supported. An unsupported provider is
        // explicitly reported, not represented as a full power-loss guarantee.
        StorageDurability.forceDirectory(stateFile.getParent());
        currentTerm = term;
        votedFor = vote;
    }

    public synchronized long currentTerm() { return currentTerm; }
    public synchronized String votedFor() { return votedFor; }

    @Override
    public synchronized void close() throws IOException {
        try { if (lock.isValid()) lock.release(); }
        finally { lockChannel.close(); }
    }
}
