package org.colchain.colchain.sparql.iter;

import org.colchain.colchain.sparql.exceptions.QueryInterruptedException;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.*;

public class ColchainJenaIterator extends NiceIterator<Pair<Triple, Binding>> {
    private final Queue<NiceIterator<Pair<Triple, Binding>>> iterators;
    private NiceIterator<Pair<Triple, Binding>> it;
    private Pair<Triple, Binding> next = null;
    private boolean empty = false;

    public ColchainJenaIterator(Queue<NiceIterator<Pair<Triple, Binding>>> iterators) {
        this.iterators = iterators;
        if(this.iterators.size() > 0)
            it = this.iterators.poll();
        else
            empty = true;
    }

    private void bufferNext() {
        if(empty) return;
        if(it.hasNext()) {
            next = it.next();
            return;
        }
        if(iterators.size() > 0) {
            it = iterators.poll();
            bufferNext();
        }
    }

    @Override
    public boolean hasNext() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if(empty) return false;
        if(next == null) bufferNext();
        return next != null;
    }

    @Override
    public Pair<Triple, Binding> next() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!hasNext())
            throw new NoSuchElementException();

        Pair<Triple, Binding> p = next;
        next = null;
        return p;
    }

}
