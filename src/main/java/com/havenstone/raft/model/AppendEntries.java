package com.havenstone.raft.model;

import java.io.Serializable;
import java.util.List;

public record AppendEntries(
    long term,
    String leaderId,
    long prevLogIndex,
    long prevLogTerm,
    List<LogEntry> entries,
    long leaderCommit
) implements Serializable{}
