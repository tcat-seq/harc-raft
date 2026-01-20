package com.havenstone.raft.storage;

import java.util.List;

import com.havenstone.raft.model.LogEntry;

/**
 * Interface for log storage in Raft.
 * Defines methods for storing and retrieving log entries,
 * as well as managing the current term and voted-for information.
 * This abstraction allows for different storage implementations
 * (e.g., in-memory, file-based, database / cloud database).
 * 
 */
public interface LogStorage {

    int getCurrentTerm();
    void setCurrentTerm(int term);

    String getVotedFor();
    void setVotedFor(String candidateId);

    LogEntry getEntry(long index);
    long getLastIndex();
    long getLastLogTerm();

    /**
     * Append new log entries to the log.
     * This should be transactional to ensure durability.
     * 
     * @param entries - list of log entries to append
     */
    void appendEntries(List<LogEntry> entries);

    /**
     * Delete all log entries from index onwards.
     * Used for log consistency checks and conflict resolution.
     * 
     * @param index - index from which to truncate the log
     */
    void truncate(long index);

}
