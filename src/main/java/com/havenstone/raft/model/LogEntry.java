package com.havenstone.raft.model;

import java.io.Serializable;

/**
 * Represents a single log entry in the Raft log.
 *
 * @param term    The term when the entry was received by the leader.
 * @param command The command to be applied to the state machine (arbitrary 
 *                  byte array/string for now).
 */
public record LogEntry(long term, String command) implements Serializable {
}
