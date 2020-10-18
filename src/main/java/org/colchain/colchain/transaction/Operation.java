package org.colchain.colchain.transaction;

import org.colchain.index.util.Triple;

import java.util.Objects;

public class Operation {
    public enum OperationType {
        ADD, DEL
    }

    private final OperationType type;
    private final Triple triple;

    public Operation(OperationType type, Triple triple) {
        this.type = type;
        this.triple = triple;
    }

    public OperationType getType() {
        return type;
    }

    public Triple getTriple() {
        return triple;
    }

    public byte[] getBytes() {
        byte b;
        if(type == OperationType.ADD)
            b = 0;
        else
            b = 1;

        byte[] subj = triple.getSubject().getBytes();
        byte[] pred = triple.getPredicate().getBytes();
        byte[] obj = triple.getObject().getBytes();

        int size = subj.length + pred.length + obj.length + 1;

        byte[] arr = new byte[size];
        arr[0] = b;
        int currIdx = 1;

        for(int i = 0; i < subj.length; i++) {
            arr[currIdx] = subj[i];
            currIdx++;
        }

        for(int i = 0; i < pred.length; i++) {
            arr[currIdx] = pred[i];
            currIdx++;
        }

        for(int i = 0; i < obj.length; i++) {
            arr[currIdx] = obj[i];
            currIdx++;
        }

        return arr;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Operation operation = (Operation) o;
        return type == operation.type &&
                Objects.equals(triple, operation.triple);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, triple);
    }
}
