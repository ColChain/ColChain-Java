package org.colchain.colchain.transaction;

import org.colchain.index.util.Triple;

import java.util.List;
import java.util.Queue;

public interface ITransaction {
    void addOperation(Operation operation);
    Operation getOperation(int index);
    List<Operation> getOperations();
    byte[] getBytes();
    String getFragmentId();
    String getAuthor();
    String getId();
    boolean isDeleted(Triple triple);
    Queue<Triple> getAdditions();
    void setTimestamp();
    long getTimestamp();
}
