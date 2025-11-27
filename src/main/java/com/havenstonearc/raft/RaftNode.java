package com.havenstonearc.raft;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.havenstonearc.raft.RaftService.AppendEntriesRequest;
import com.havenstonearc.raft.RaftService.AppendEntriesResponse;
import com.havenstonearc.raft.RaftService.VoteRequest;
import com.havenstonearc.raft.RaftService.VoteResponse;

/**
 * Represents a node in the Raft consensus algorithm. 
 * It implements the core state machine logic including leader election, 
 * log replication, and hearbeat logic.
 * 
 */
public class RaftNode implements AutoCloseable, RaftService.RaftMessageReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
    private final RaftService rpcService;
    private final Log persistentLog;

    // Node identity
    private final int id;
    private final List<Integer> peerIds;

    private final ScheduledExecutorService scheduler;
    private final ReentrantLock lock = new ReentrantLock();
    private volatile State state = State.FOLLOWER;

    // Timers
    private Future<?> electionTimer;
    private Future<?> heartbeatTimer;
    private final Random random = new Random();

    private AtomicLong currentTerm = new AtomicLong(0);
    private volatile Integer votedFor = null;
    private volatile long commitIndex = 0;
    private final Set<Integer> votesReceived = new HashSet<>();

    // configuration
    private final long minElectionTimeoutMs = 300;
    private final long maxElectionTimeoutMs = 600;
    private final long heartbeatIntervalMs = 150;

    public RaftNode(int id, List<Integer> allIds, RaftService rpcService, Log persistentLog) {
        this.id = id;
        this.peerIds = new ArrayList<>(allIds);
        this.peerIds.remove((Integer) id); // faster to remove using the remove method instead of stream filtering.
        this.rpcService = rpcService;
        this.persistentLog = persistentLog;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Node-" + id + "-Scheduler");
            t.setDaemon(true);
            return t;
        });        
    }

    public void start() {
        try {
            // Initiatialize persistent log
            persistentLog.initialize();

            // if currentTerm is 0, this is the first start. Else we are recovering from a crash
            if (persistentLog.getLastIndex() >= 0) {
                currentTerm.set(persistentLog.getTerm(persistentLog.getLastIndex()));
            }

            rpcService.start(this);

            resetElectionTimeout();
            LOGGER.info("Node {} started with term {}", id, currentTerm.get());
        } catch (Exception e) {
            LOGGER.error("Failed to start Raft node", e);
            throw new RuntimeException("Failed to start Raft node", e);
        }
    }

    @Override
    public VoteResponse handleRequestVote(VoteRequest request) {
        lock.lock();
        try {
            long localTerm = currentTerm.get();

            int requestCandidateId = request.candidateId();
            long requestTerm = request.term();

            LOGGER.info("Node {} received VoteRequest from Node {} for term {} (local term {})",
                        id, requestCandidateId, requestTerm, localTerm);

            // do not vote for candidate if their request term was lower than the local term
            if (requestTerm < localTerm) {
                LOGGER.info("Node {} rejecting VoteRequest from Node {} due to their term {} being lower than the local term {}", 
                                    this.id, requestCandidateId, requestTerm, localTerm);

                // return the higher local term and the vote denial (false)
                return new VoteResponse(localTerm, false, id);
            }

            // if the request term is higher than local term, update this node's current term 
            // to the term received and convert this node to FOLLOWER
            if (requestTerm > localTerm) {
                stepDownToFollower(requestTerm);

                LOGGER.info("Node {} updated current term to {} and converted to FOLLOWER due to higher term in VoteRequest from Node {}",
                            id, requestTerm, requestCandidateId);
            }

            // check if this node has already voted in this term
            boolean logIsUpToDate = isLogUpToDate(request.lastLogIndex(), request.lastLogTerm());
            if ((votedFor == null || votedFor == requestCandidateId) && logIsUpToDate) {
                // grant vote to candidate
                votedFor = requestCandidateId;

                // reset election timeout
                resetElectionTimeout();

                LOGGER.info("Node {} granted vote to Node {} for term {}", id, requestCandidateId, requestTerm);
                return new VoteResponse(requestTerm, true, id);
            }

            // Either already voted for someone else or candidate's log is not up-to-date. Reject vote.
            LOGGER.info("Node {} rejecting VoteRequest from Node {} for term {}. Already voted for Node {} or candidate log is not up-to-date",
                        id, requestCandidateId, requestTerm, votedFor);
            return new VoteResponse(requestTerm, false, id);
        } finally {
            lock.unlock();
        }
        
    }

    @Override
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {

        throw new UnsupportedOperationException("Unimplemented method 'handleAppendEntries'");
    }

    /**
     * Steps down this node to FOLLOWER for the given term
     * 
     * @param requestTerm
     */
    private void stepDownToFollower(long requestTerm) {
        currentTerm.set(requestTerm);
        votedFor = null;
        state = State.FOLLOWER;

        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }

        resetElectionTimeout();
    }

    /**
     * Reset the election timeout timer
     * 
     */
    private void resetElectionTimeout() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }
        long delay = minElectionTimeoutMs + random.nextInt((int) (maxElectionTimeoutMs - minElectionTimeoutMs));

        // start new election timer
        electionTimer = scheduler.schedule(this::startElection, delay, TimeUnit.MILLISECONDS);
    }

    /**
     * Start a new election - transtion to CANDIDATE, increment 
     * term, vote for self, send VoteRequest to peers.
     * 
     */
    private void startElection() {
        lock.lock();
        try {
            if (state == State.LEADER) {
                // already a leader. No need to start election
                return;
            }

            state = State.CANDIDATE;
            long newTerm = currentTerm.incrementAndGet();
            votedFor = id;

            // reset votes received set and add self-vote
            votesReceived.clear();
            votesReceived.add(id);

            LOGGER.info("Node {} starting election for term {}", id, newTerm);

            resetElectionTimeout();

            // Send VoteRequest to all peers
            VoteRequest req = new VoteRequest(newTerm, id, persistentLog.getLastIndex(), persistentLog.getTerm(persistentLog.getLastIndex()));
            for (int peerId : peerIds) {
                rpcService.sendVoteRequest(peerId, req)
                            .thenAccept(res -> {
                                if (res != null) {
                                    handleVoteResponse(res);
                                }
                            })
                            .exceptionally(ex -> {
                                LOGGER.error("Node {} failed to send VoteRequest to Node {}", id, peerId, ex);
                                return null;
                            });

            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Handle VoteResponse from a peer.
     * 
     * @param response
     */
    private void handleVoteResponse(VoteResponse response) {
        lock.lock();
        try {
            if (state != State.CANDIDATE) {
                // Node is no longer a candidate. Ignore response.
                return;
            }

            if (response.term() > currentTerm.get()) {
                // Vote Response indicates a higher term. Step down to FOLLOWER
                stepDownToFollower(response.term());

                LOGGER.info("This candidate Node {} received vote response with a higher term {}. Therefore stepping dow to FOLLOWER", id, response.term());
                return;
            }

            if (response.term() == currentTerm.get() && response.voteGranted()) {
                // add the peer id that granted the vote to the 
                // 'votesReceived' Set to ensure unique votes
                votesReceived.add(response.peerId());
                LOGGER.info("This candidate Node {} received vote from Node {} for term {}. Total votes so far: {}",
                            id, response.peerId(), response.term(), votesReceived.size());

                int votes = votesReceived.size(); // includes self-vote

                // calculate majority
                int majorityVotesNeeded = (peerIds.size() + 1)/2 + 1; // +1 for self
                if (votes >= majorityVotesNeeded) {
                    becomeLeader();
                }
            }

        } finally {
            lock.unlock();
        }
    }


    private void becomeLeader() {
        if (state == State.LEADER) {
            // already a leader, return.
            return; 
        }

        state = State.LEADER;
        LOGGER.info("Node {} has become LEADER for term {}", id, currentTerm.get());

        // cancel election timer
        if (electionTimer != null) {
            electionTimer.cancel(false);
        }

        // start sending heartbeats to FOLLOWERS
        sendHeartBeats(); // send initial heartbeat immediately
        heartbeatTimer = scheduler.scheduleAtFixedRate(this::sendHeartBeats, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Send heartbeats (AppendEntriesRequest with no log entries) to all Followers
     * 
     */
    private void sendHeartBeats() {
        lock.lock();

        try {
            // if no longer a leader, cancel heartbeat timer and return
            if (state != State.LEADER) {
                if (heartbeatTimer != null) {
                    heartbeatTimer.cancel(false);
                    return;
                }
            }

            AppendEntriesRequest heartbeatMsg = new AppendEntriesRequest(currentTerm.get(), id, persistentLog.getLastIndex(), 0, List.of(), commitIndex);
            for (int peerId : peerIds) {
                rpcService.sendAppendEntries(peerId, heartbeatMsg)
                            .thenAccept(res -> {
                                // if response term is higher, step down LEADER to FOLLOWER 
                                if (res != null && res.term() > currentTerm.get()) {
                                    lock.lock();
                                    try {
                                        stepDownToFollower(res.term());
                                        LOGGER.info("Leader Node {} received higher term {} AppendEntriesResponse heartbeat response from Node {}. Therefore stepping down to FOLLOWER", 
                                                    this.id, res.term(), peerId);
                                    } finally {
                                        lock.unlock();
                                    }
                                }
                            })
                            .exceptionally(ex -> {
                                LOGGER.error("Leader Node {} failed to send hearrbeat to Node {}", id, peerId, ex);
                                return null;
                            });
            }

        } finally {
            lock.unlock();
        }

    }

    private boolean isLogUpToDate(long candidateLastLogIndex, long candidateLastLogTerm) {
        long localLastLogIndex = persistentLog.getLastIndex();
        long localLastLogTerm = (localLastLogIndex >=0) ? persistentLog.getTerm(localLastLogIndex) : 0;

        // compare terms. Return true if candidate's term is higher
        if (candidateLastLogTerm != localLastLogTerm) {
            return candidateLastLogTerm > localLastLogTerm;
        }
        // terms are equal in the above condition, compare log indices  
        return candidateLastLogIndex >= localLastLogIndex;
    }

    @Override
    public void close() throws Exception {
        // Cleanup resources
    }


}
