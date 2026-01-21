package com.havenstone.raft.node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.havenstone.raft.storage.LogStorage;
import com.havenstone.raft.transport.NetworkTransport;

public class RaftNode {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    // Node configuration
    private final String nodeId;
    private final List<String> peerIds; // IDs of other nodes in the cluster
    private final NetworkTransport transport;
    private final LogStorage storage;

    // Volatile state on all servers
    private long commitIndex = 0;
    private long lastApplied = 0;
    private Role currentRole = Role.FOLLOWER;

    // Leader state (reinitialized after election)
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Election Timer state
    private long lastHeartbeatTime;
    private final long electionTimeoutMillis;

    // Virtual Thread executor for handling RPCs async
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private final ReentrantLock lock = new ReentrantLock();

    public RaftNode(String nodeId, List<String> peerIds, NetworkTransport transport, LogStorage storage) {
        this.nodeId = nodeId;
        this.peerIds = peerIds;
        this.transport = transport;
        this.storage = storage;
        // Raft requires randomized election timeouts between 150ms and 300ms 
        // to prevent split votes
        this.electionTimeoutMillis = 150 + (long)(Math.random() * 150); 
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    /**
     * Starts the main loop of the node in a Virtual Thread.
     * 
     */
    public void start() {
        executor.submit(this::runMainLoop);
        logger.info("Node {} started as FOLLOWER with timeout {}ms", nodeId, electionTimeoutMillis);
    }


    private void runMainLoop() {
        while (true) {
            try {
                // Sleep briefly to avoid CPU spinning and 
                // allow other virtual threads to run
                Thread.sleep(10);

                lock.lock();
                try {
                    long currentTime = System.currentTimeMillis();
                    if (currentRole == Role.LEADER) {
                        // Leaders send heartbeats periodically
                        sendHeartBeats();
                    } else {
                        // Followers and Candidates become candidates if election timeout elapses
                        if (currentTime - lastHeartbeatTime >= electionTimeoutMillis) {
                            startElection();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                logger.error("Node {} main loop interrupted: {}", nodeId, e.getMessage());
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Node {} encountered error in main loop: {}", nodeId, e.getMessage());
            }
        }
    }

    private void startElection() {
        logger.info("Election timeout reached. Node {} starting election", nodeId);

        // TODO: Implement election logic

    }

    private void sendHeartBeats() {
        // TODO: Implement heartbeat logic
    }

}
