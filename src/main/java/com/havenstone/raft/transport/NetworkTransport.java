package com.havenstone.raft.transport;

import java.util.concurrent.CompletableFuture;

import com.havenstone.raft.model.AppendEntries;
import com.havenstone.raft.model.AppendEntriesResult;
import com.havenstone.raft.model.RequestVote;
import com.havenstone.raft.model.RequestVoteResult;

/**
 * Interface for network transport layer to send RPCs between Raft nodes.
 * Allows for abstraction over different transport mechanisms (e.g., gRPC, 
 * HTTP, TCP sockets, etc.).
 * Defines methods to send RequestVote and AppendEntries RPCs asynchronously.
 * 
 */
public interface NetworkTransport {

    /**
     * Sends a RequestVote RPC to a specified peer.
     * Use CompletableFuture to handle the response asynchronously 
     * on a Virtual Thread.
     * 
     * @param peerId - ID of the peer to send the request to
     * @param request - RequestVote
     * 
     * @return CompletableFuture<RequestVoteResult>
     */
    CompletableFuture<RequestVoteResult> sendRequestVote(String peerId, RequestVote request);

    /**
     * Sends an AppendEntries RPC to a specified peer.
     * Use CompletableFuture to handle the response asynchronously on a Virtual Thread
     * 
     * @param peerId - ID of the peer to send the request to
     * @param request - AppendEntries request
     * @return CompletableFuture<AppendEntriesResult>
     */
    CompletableFuture<AppendEntriesResult> sendAppendEntries(String peerId, AppendEntries request);
}
