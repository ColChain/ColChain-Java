package org.colchain.colchain.transaction.impl;

import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;

import java.util.List;

public class TransactionFactory {
    public static ITransaction getTransaction(String id, String author) {
        return new TransactionImpl(id, author);
    }

    public static ITransaction getTransaction(Operation op, String id, String author) {
        return new TransactionImpl(op, id, author);
    }

    public static ITransaction getTransaction(List<Operation> ops, String id, String author) {
        return new TransactionImpl(ops, id, author);
    }

    public static ITransaction getTransaction(String fid, String author, String id) {
        return new TransactionImpl(fid, author, id);
    }

    public static ITransaction getTransaction(Operation op, String fid, String author, String id) {
        return new TransactionImpl(op, fid, author, id);
    }

    public static ITransaction getTransaction(List<Operation> ops, String fid, String author, String id) {
        return new TransactionImpl(ops, fid, author, id);
    }

    public static ITransaction getTransaction(List<Operation> ops, String fid, String author, String id, long timestamp) {
        return new TransactionImpl(ops, fid, author, id, timestamp);
    }
}
