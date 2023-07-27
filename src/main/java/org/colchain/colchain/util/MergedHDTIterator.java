package org.colchain.colchain.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class MergedHDTIterator<T> implements Iterator<T> {
    private final Queue<T> queue;
    private T next = null;

    public MergedHDTIterator(Collection<T> itQueue) {
        this.queue = new LinkedList<>(itQueue);
    }

    public MergedHDTIterator() {
        this.queue = new LinkedList<>();
    }

    @Override
    public boolean hasNext() {
        if(next == null) bufferNext();
        return next != null;
    }

    @Override
    public T next() {
        if(next == null) bufferNext();
        T nxt = next;
        next = null;
        return nxt;
    }

    private void bufferNext() {
        if (next != null) return;
        if(queue.size() > 0)
            next = queue.poll();
    }
}
