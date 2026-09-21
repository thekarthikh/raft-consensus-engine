package com.raft;

import com.google.protobuf.ByteString;
import com.raft.rpc.RaftServiceGrpc;
import com.raft.rpc.SubmitCommandRequest;
import com.raft.rpc.SubmitCommandResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.TimeUnit;

/** Minimal write client; follower rejection is explicit, and unknown outcomes are not retried. */
public final class RaftClientApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("Usage: RaftClientApp <host> <port> <command>");
        ManagedChannel channel = ManagedChannelBuilder.forAddress(args[0], Integer.parseInt(args[1])).usePlaintext().build();
        try {
            SubmitCommandResponse response = RaftServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .submitCommand(SubmitCommandRequest.newBuilder().setData(ByteString.copyFromUtf8(args[2])).build());
            if (!response.getCommitted()) throw new IllegalStateException("Not leader; known leader=" + response.getLeaderId());
            System.out.printf("COMMITTED leader=%s term=%d index=%d%n", response.getLeaderId(), response.getTerm(), response.getIndex());
        } finally { channel.shutdownNow(); channel.awaitTermination(3, TimeUnit.SECONDS); }
    }
}
