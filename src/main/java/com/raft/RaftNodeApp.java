package com.raft;

import com.raft.core.RaftNode;

import java.util.Arrays;
import java.util.List;

public class RaftNodeApp {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {

            System.out.println(
                    "Usage: java -jar app.jar <nodeId> <port> [node1=host:port,node2=host:port,...]"
            );

            System.exit(1);
        }

        String nodeId = args[0];

        int port = Integer.parseInt(args[1]);

        List<String> nodes = args.length >= 3 ? Arrays.asList(args[2].split(",")) :
                Arrays.asList(
                        "node1",
                        "node2",
                        "node3"
                );

        RaftNode node =
                new RaftNode(
                        nodeId,
                        port,
                        nodes
                );

        node.start();

        node.blockUntilShutdown();
    }
}
