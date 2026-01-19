package com.havenstone.raft.model;

import java.io.Serializable;

/**
 * Represents a RequestVote RPC. Invoked by candidates to gather votes.
 * 
 * @param term         Candidate’s term
 * @param candidateId  Candidate requesting vote
 * @param lastLogIndex Index of candidate’s last log entry
 * @param lastLogTerm  Term of candidate’s last log entry
 */
public record RequestVote(
    long term,
    String candidateId,
    long lastLogIndex,
    long lastLogTerm
) implements Serializable{}
