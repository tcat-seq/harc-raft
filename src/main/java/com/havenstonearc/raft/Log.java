package com.havenstonearc.raft;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Interface representing the persistent log storage for Raft nodes.
 * 
 */
public interface Log {

    /**
     * Represents a single log entry in the replicated Raft log.
     * Must be seriablizable to allow for storage & transmission over the network.
     */
    record LogEntry(long term, String command) implements Serializable {}

    LogEntry getEntry(long index);
    long getTerm(long index);
    long getLastIndex();
    void appendEntries(List<LogEntry> entries) throws IOException;
    void truncate(long index) throws IOException;
    void initialize() throws IOException;

}
