package com.havenstone.raft.model;

import java.io.Serializable;
import java.util.List;

/**
 * Represents an AppendEntries RPC. Invoked by leader to replicate log entries
 * and to provide a heartbeat.
 * 
 * @param term         Leader’s term
 * @param leaderId     So follower can redirect clients
 * @param prevLogIndex Index of log entry immediately preceding new ones
 * @param prevLogTerm  Term of prevLogIndex entry
 * @param entries      Log entries to store (empty for heartbeat; may send 
 *                      more than one for efficiency)
 * @param leaderCommit Leader’s commitIndex
 */
public record AppendEntries(
    long term,
    String leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit
) implements Serializable{}
