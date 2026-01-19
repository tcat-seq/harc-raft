package com.havenstone.raft.model;

import java.io.Serializable;

/**
 * Represents the result of a RequestVote RPC.
 * 
 * @param term        Current term, for candidate to update itself.
 * @param voteGranted True means candidate received vote.
 */
public record RequestVoteResult(
    long term, 
    boolean voteGranted
) implements Serializable{}
