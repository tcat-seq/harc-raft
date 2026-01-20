package com.havenstone.raft.model;

import java.io.Serializable;

/**
 * Represents the result of an AppendEntries RPC.
 * 
 * @param term    Current term, for leader to update itself.
 * @param success True if follower contained entry matching prevLogIndex and 
 *                  prevLogTerm.
 */
public record AppendEntriesResult(
    long term,
    boolean success
) implements Serializable{}
