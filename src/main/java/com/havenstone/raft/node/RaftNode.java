package com.havenstone.raft.node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.havenstone.raft.model.AppendEntries;
import com.havenstone.raft.model.AppendEntriesResult;
import com.havenstone.raft.model.RequestVote;
import com.havenstone.raft.model.RequestVoteResult;
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
        // to prevent split votes. If two nodes wake up at the exact same time to
        // become candidates, they may keep splitting votes indefinitely.
        // Without randomness, this can lead to livelock. Randomization ensures
        // one node times out slightly earlier, wins the election, and establishes 
        // leadership.
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


    /**
     * Main loop for the Raft node.
     * Handles role-specific actions such as sending heartbeats
     * for leaders and starting elections for followers/candidates.
     * Runs indefinitely in a Virtual Thread.
     */
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

    /**
     * Starts a new election by transitioning to Candidate role,
     * incrementing current term, voting for self, and broadcasting
     * RequestVote RPCs to all peers asynchronously using Virtual Threads.
     * 
     */
    private void startElection() {
        logger.info("Election timeout reached. Node {} starting election", nodeId);

        currentRole = Role.CANDIDATE;
        storage.setCurrentTerm(storage.getCurrentTerm() + 1);
        storage.setVotedFor(this.nodeId);
        // reset timer
        lastHeartbeatTime = System.currentTimeMillis();

        long termAtElectionStart = storage.getCurrentTerm();
        // Vote for self
        AtomicLong votesReceived = new AtomicLong(1);

        // Broadcast RequestVote in parallel using VTs to all peers
        for (String peerId : peerIds) {
            executor.submit(() -> {
                RequestVote rv = new RequestVote(
                    termAtElectionStart, 
                    this.nodeId,
                    storage.getLastIndex(),
                    storage.getLastLogTerm()
                );

                transport.sendRequestVote(peerId, rv).thenAccept(response ->{
                    lock.lock();
                    try {
                        // First check if still a candidate and term has not changed
                        if (currentRole != Role.CANDIDATE || storage.getCurrentTerm() != termAtElectionStart) {
                            // No longer a candidate (election is over) or term has changed
                            return;
                        }

                        // Process response if still a candidate
                        // First check for higher term from peer in response.
                        // If higher term, step down to follower
                        if (response.term() > storage.getCurrentTerm()) {
                            becomeFollower(response.term());
                            return;
                        }

                        // Process response.
                        // If vote granted from peer response, increment vote count.
                        // If majority reached, become Leader.
                        // Majority is (N/2)+1 where N is total nodes including self.
                        // Here, peerIds.size() + 1 accounts for self vote.
                        // If total nodes is even, integer division handles floor automatically.
                        // E.g., for 4 nodes, majority is (4/2)+1 = 3. For 5 nodes, (5/2)+1 = 3.
                        // This ensures correct majority calculation for both even and odd cluster sizes.
                        // If majority reached, transition to LEADER role and send initial heartbeats.
                        if (response.voteGranted()) {
                            long totalVotes = votesReceived.incrementAndGet();

                            if (totalVotes > (peerIds.size() + 1) / 2) {
                                becomeLeader();
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }); // end of transport.sendRequestVote(...).thenAccept
            }); // end of executor.submit for each peerId
        } // end of for loop over peerIds

    }

    /**
     * Transitions the node to Leader role after winning an election
     * Sets up leader state and sends initial heartbeats.
     * 
     */
    private void becomeLeader() {
        // Won the election. Transition to Leader role.
        currentRole = Role.LEADER;

        // Reinitialize leader state
        for (String pid : peerIds) {
            nextIndex.put(pid, storage.getLastIndex() + 1);
            matchIndex.put(pid, 0L);
        }
        logger.info("Node {} became LEADER for term {}", nodeId, storage.getCurrentTerm());

        // Immediately send initial heartbeats
        sendHeartBeats();
    }

    /**
     * Steps down to follower role upon discovering a higher term.
     * 
     * Term Logic:
     * - Raft relies on currentTerm (a monotonically increasing integer). 
     *   It acts as a logical clock to order events and ensure consistency.
     * - Anytime a node discovers a term higher than its currentTerm, it must
     *   immediately step down to follower role. This handles network partitions
     *   where an old leader may still think it's the leader but other nodes 
     * have moved on to a higher term.
     * 
     * @param higherTerm - the higher term, discovered from a peer response
     */
    private void becomeFollower(long higherTerm) {
        // Discovered higher term, step down to follower
        storage.setCurrentTerm(higherTerm);
        currentRole = Role.FOLLOWER;
        storage.setVotedFor(null);
        this.lastHeartbeatTime = System.currentTimeMillis();

        logger.info("Node {} stepped down to FOLLOWER due to higher term {}", nodeId, higherTerm);
    }

    /**
     * Send heartbeats (AppendEntries RPCs with no log entries for now) to all 
     * peers asynchronously. In a full implementation, would also handle log 
     * replication.
     * 
     * Heartbeats mechanism:
     * - In Raft, the leader sends periodic heartbeats to all followers to
     *  maintain its authority and prevent followers from starting new elections.
     * It does this by sending AppendEntries RPCs with no log entries.
     * - Heartbeats serve two main purposes:
     * 1. Assert Leadership: By sending regular heartbeats, the leader
     *    asserts its leadership over the cluster. Followers reset their election
     *    timers (electionTimeoutMillis) upon receiving heartbeats, preventing them
     *    from starting new elections.
     * 2. Maintain Log Consistency: Even when there are no new log entries to 
     *    replicate, heartbeats help ensure that followers' logs remain consistent
     *    with the leader's log. This is crucial for maintaining the integrity of the 
     *    distributed system.
     *  
     * 
     * 
     * 
     */
    private void sendHeartBeats() {
        logger.debug("Leader {} sending heartbeats", nodeId);
        
        long term = storage.getCurrentTerm();

        // NOTE: Focusing on empty hearbeats for now.
        // In a full implementation, would also send log entries as needed.
        for (String peerId : peerIds) {
            executor.submit(() -> {
                
                // Construct AppendEntries instance with empty heartbeat entries for now.
                // In a full implementation, would include log entries as needed.

                // The purpose of prevLogIndex and prevLogTerm is to ensure log consistency
                // during log replication (AppendEntries RPC), where the leader sends
                // it to the follower to verify that the follower's log matches the 
                // leader's log up to that specific point before appending new entries,
                // ensuring that logs remain consistent across the servers in the cluster
                // and preventing data loss or divergence.
                // It tells the follower the index of the log entry just before the new 
                // ones (prevLogIndex), requiring the follower to confirm both the index and its 
                // associated term (prevLogTerm) match before accepting. 
                // How it works:
                // 1. Leader Sends: When a leader sends an AppendEntries RPC (or hearrbeat), 
                //   it includes prevLogIndex (index of the log entry before the new ones) 
                //   and prevLogTerm (the term of that entry) in the request.
                // 2. Follower Validates: Upon receiving the AppendEntries RPC (or heartbeat), 
                //   the follower checks: "Does my log contain an entry at prevLogIndex 
                //   with term equal to prevLogTerm?"
                // 3. Consistency Check:
                //   - If Yes (Success): The follower's log is consistent with the leader's 
                //     log up to that point. The follower can safely append the new entries
                //     sent by the leader.
                //   - If No (Failure): There is a log inconsistency. The follower rejects 
                //     the AppendEntries RPC (or heartbeat) and does not append the new entries.
                //     The leader will need to retry with earlier log entries until consistency is restored.
                // 4. Log Repair: If inconsistencies are detected, the leader will decrement 
                //    nextIndex for that follower and retry sending AppendEntries RPCs
                //    (or heartbeats) with earlier log entries until the follower's log
                //    matches the leader's log at prevLogIndex and prevLogTerm.
                // 5. Continuing Replication: Once consistency is restored, the leader can
                //    continue sending new log entries to the follower.
                // This mechanism ensures that all followers' logs remain consistent with the leader's log,
                // which is crucial for maintaining the integrity and reliability of the distributed system.
                //
                // For heartbeats with no new entries, these still need to be sent
                // to maintain the log consistency checks.
                long prevLogIndex = nextIndex.getOrDefault(peerId, 1L) - 1;
                long prevLogTerm = (prevLogIndex == 0) ? 0 : storage.getEntry(prevLogIndex).term();
                AppendEntries ae = new AppendEntries(
                    term,
                    this.nodeId,
                    prevLogIndex,
                    prevLogTerm,
                    List.of(), // empty entries for heartbeat
                    this.commitIndex
                );

                transport.sendAppendEntries(peerId, ae).thenAccept(response -> {
                    lock.lock();
                    try {
                        // First check if still leader and term has not changed
                        if (currentRole != Role.LEADER || storage.getCurrentTerm() != term) {
                            // No longer a leader (stepped down) or term has changed
                            return;
                        }

                        // Process response if still a leader
                        // First check for higher term from peer in response.
                        // If higher term, step down to follower
                        if (response.term() > storage.getCurrentTerm()) {
                            becomeFollower(response.term());
                            return;
                        }

                        // For heartbeats, we don't need to update nextIndex/matchIndex
                        // since no log entries are sent.
                        // In a full implementation, would handle 
                        // log replication here.

                    } finally {
                        lock.unlock();
                    }
                }); // end of transport.sendAppendEntries(...).thenAccept
            }); // end of executor.submit for each peerId
        } // end of for loop over peerIds

    }


    // --- RPC Handlers (called by NetworkTransport implementation) --- 

    /**
     * Handles incoming vote requests (RequestVote RPC) from Candidate.
     * 
     * Follows Raft voting rules to decide whether to grant the vote.
     * 1. If the request's term is less than currentTerm, reject the vote.
     * 2. If the request's term is greater than currentTerm, step down to
     *    follower and consider the vote.
     * 3. If haven't voted yet in this term (or voted for the requesting 
     *    candidate), check if candidate's log is at least as up-to-date 
     *    as receiver's log. 
     *      If so, grant the vote.
     * 4. Reset self election timer if vote granted.
     *
     * @param request - RequestVote RPC from candidate
     * @return RequestVoteResult indicating whether vote was granted
     */
    public RequestVoteResult handleRequestVote(RequestVote request) {
        lock.lock();
        try {
            long currentTerm = storage.getCurrentTerm();
            boolean voteGranted = false;

            // If request term is less than current term, reject vote
            if (request.term() < currentTerm) {
                logger.info("Node {} rejecting vote for {} due to lower term -  {} (Request Candidate Term) <  {} (Receiving Node Term)", 
                    nodeId, request.candidateId(), request.term(), currentTerm);

                return new RequestVoteResult(currentTerm, false);
            }

            // If request term is greater than current term, step down 
            // to follower
            if (request.term() > currentTerm) {
                logger.info("Node {} stepping down to FOLLOWER due to higher term in RequestVote from {} (Request Candidate Node Id). {} (Request Candidate Term) >  {} (Receiving Node Term)", 
                    nodeId, request.candidateId(), request.term(), currentTerm);

                becomeFollower(request.term());
                currentTerm = request.term();
            }

            // Check if already voted in this term (or if we have voted for 
            // the requesting candidate)
            String votedFor = storage.getVotedFor();
            if (votedFor == null || votedFor.equals(request.candidateId())) {
                // Check candidate's log is at least as up-to-date as receiver's log.
                // See the comments in sendHeartBeats() for explanation of log consistency check.
                long lastLogIndex = storage.getLastIndex();
                long lastLogTerm = storage.getLastLogTerm();

                // Request Candidate's log is at least as up-to-date as receiver's log
                // if its last log term is greater than receiver's last log term,
                // or if the terms are equal and candidate's last log index is
                // greater than or equal to receiver's last log index.
                // This ensures that votes are granted to candidates with
                // the most complete logs, maintaining log consistency across the 
                // cluster.
                boolean isLogOk = (request.lastLogTerm() > lastLogTerm) ||
                                (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);

                if (isLogOk) {
                    // Grant vote
                    storage.setVotedFor(request.candidateId());
                    voteGranted = true;
                    // Reset election timer
                    lastHeartbeatTime = System.currentTimeMillis();
                }
            }

            return new RequestVoteResult(currentTerm, voteGranted);

        } finally {
            lock.unlock();
         }

    }


    public AppendEntriesResult handleAppendEntries(AppendEntries request) {
        lock.lock();
        try { 
            long currentTerm = storage.getCurrentTerm();

            // If request term is less than current term, reject append and 
            // return current term so leader can update itself to higher term and
            // step down if needed.
            if (request.term() < currentTerm) {
                logger.info("Node {} rejecting AppendEntries from {} (candidate) due to lower term.  {} (Request Leader Term) <  {} (Receiving Node Term)", 
                    nodeId, request.leaderId(), request.term(), currentTerm);

                return new AppendEntriesResult(currentTerm, false);
            }

            // If request term is greater than current term, step down 
            // to follower
            if (request.term() > currentTerm && currentRole != Role.FOLLOWER) {
                logger.info("Node {} stepping down to FOLLOWER due to higher term in AppendEntries from {} (Request Leader Node Id). {} (Request Leader Term) >  {} (Receiving Node Term)", 
                    nodeId, request.leaderId(), request.term(), currentTerm);

                becomeFollower(request.term());
                currentTerm = request.term();
            }

            // Reset election timer on receiving valid AppendEntries
            lastHeartbeatTime = System.currentTimeMillis();

            // For simplicity, not implementing full log consistency checks here.
            // In a full implementation, would check prevLogIndex and prevLogTerm,
            // append new entries, and update commitIndex as needed.
            // TODO: Implement full log consistency checks and log replication.

            return new AppendEntriesResult(currentTerm, true);

        } finally {
            lock.unlock();
        }
        
    }


}
