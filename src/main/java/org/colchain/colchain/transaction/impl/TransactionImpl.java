package org.colchain.colchain.transaction.impl;

import org.colchain.index.util.Triple;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.util.RandomString;

import java.util.*;

public class TransactionImpl implements ITransaction {
    private List<Operation> operations = new ArrayList<>();
    private final String fragmentId;
    private final String author;
    private final String id;
    private long timestamp = 0;
    private static RandomString gen = new RandomString();

    TransactionImpl(List<Operation> ops, String id, String author) {
        this.operations = ops;
        this.fragmentId = id;
        this.author = author;
        this.id = gen.nextString();
    }

    TransactionImpl(Operation op, String id, String author) {
        operations.add(op);
        this.fragmentId = id;
        this.author = author;
        this.id = gen.nextString();
    }

    TransactionImpl(String id, String author) {
        this.fragmentId = id;
        this.author = author;
        this.id = gen.nextString();
    }

    TransactionImpl(List<Operation> ops, String fid, String author, String id) {
        this.operations = ops;
        this.fragmentId = fid;
        this.author = author;
        this.id = id;
    }

    TransactionImpl(List<Operation> ops, String fid, String author, String id, long timestamp) {
        this.operations = ops;
        this.fragmentId = fid;
        this.author = author;
        this.id = id;
        this.timestamp = timestamp;
    }

    TransactionImpl(Operation op, String fid, String author, String id) {
        operations.add(op);
        this.fragmentId = fid;
        this.author = author;
        this.id = id;
    }

    TransactionImpl(String fid, String author, String id) {
        this.fragmentId = fid;
        this.author = author;
        this.id = id;
    }

    @Override
    public void setTimestamp() {
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public String getAuthor() {
        return author;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isDeleted(Triple triple) {
        for (Operation op : operations) {
            if (op.getTriple().equals(triple) && op.getType() == Operation.OperationType.DEL) return true;
        }
        return false;
    }

    @Override
    public Queue<Triple> getAdditions() {
        Queue<Triple> ret = new LinkedList<>();

        for(Operation op : operations) {
            if(op.getType() == Operation.OperationType.ADD) ret.add(op.getTriple());
        }

        return ret;
    }

    @Override
    public void addOperation(Operation operation) {
        operations.add(operation);
    }

    @Override
    public Operation getOperation(int index) {
        return operations.get(index);
    }

    @Override
    public List<Operation> getOperations() {
        return operations;
    }

    @Override
    public String getFragmentId() {
        return fragmentId;
    }

    @Override
    public byte[] getBytes() {
        int size = 0;

        for (Operation op : operations) {
            size += op.getBytes().length;
        }

        byte[] arr = new byte[size];
        int currIdx = 0;

        for (Operation op : operations) {
            byte[] bs = op.getBytes();
            for (byte b : bs) {
                arr[currIdx] = b;
                currIdx++;
            }
        }

        return arr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransactionImpl that = (TransactionImpl) o;
        return Objects.equals(operations, that.operations) &&
                Objects.equals(fragmentId, that.fragmentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operations, fragmentId);
    }
}
