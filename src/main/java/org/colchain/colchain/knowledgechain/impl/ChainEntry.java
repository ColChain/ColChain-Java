package org.colchain.colchain.knowledgechain.impl;

import org.colchain.colchain.transaction.ITransaction;

public class ChainEntry {
    private final ITransaction transaction;
    private final ChainEntry prev;

    public ChainEntry(ITransaction transaction, ChainEntry prev) {
        this.transaction = transaction;
        this.prev = prev;
    }

    private ChainEntry() {
        transaction = null;
        prev = null;
    }

    public static ChainEntry getInitialEntry() {
        return new ChainEntry();
    }

    public ITransaction getTransaction() {
        return transaction;
    }

    public ChainEntry previous() {
        return prev;
    }

    public boolean isFirst() {
        return transaction == null && prev == null;
    }
}
