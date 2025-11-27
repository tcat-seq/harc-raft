package com.havenstonearc.raft;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.havenstonearc.raft.Log.LogEntry;

/**
 * Represents the service interface for Raft nodes to communicate with 
 * each other.
 * Acts as a Client to send messages to peer Raft nodes. 
 * Requires a "receiver" (RaftNode) to handle incoming messages from 
 * peer Raft nodes.
 * 
 */
public interface RaftService {

    // RPC message objects (need to be serializable for network transmission)
    record VoteRequest(long term, int candidateId, long lastLogIndex, 
                        long lastLogTerm) implements Serializable {}
    record VoteResponse(long term, boolean voteGranted, int peerId) implements Serializable {}

    record AppendEntriesRequest(long term, int leaderId, long prevLogIndex,
                                long prevLogTerm, List<LogEntry> entries,
                                long leaderCommit) implements Serializable {}                            
    record AppendEntriesResponse(long term, boolean success, long matchIndex) 
                                implements Serializable {}

    // Client methods to send messages to other nodes

    /**
     * Send a VoteRequest to a specific peer asyc
     * 
     * @param targetPeerId
     * @param request
     * @return
     */
    CompletableFuture<VoteResponse> sendVoteRequest(int targetPeerId,
                                                    VoteRequest request);

    /**
     * Send an AppendEntriesRequest to a specific peer asynchronously
     */
    CompletableFuture<AppendEntriesResponse> sendAppendEntries(int targetPeerId, 
                                                                AppendEntriesRequest request);


    // Server lifecycle methods
    
    /**
     * Start the Raft service to listen to and handle incoming messages
     *  
     * @param receiver
     */
    void start(RaftMessageReceiver receiver);
    void stop();

    interface RaftMessageReceiver {
        VoteResponse handleRequestVote(VoteRequest request);
        AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);
    }
    

}
