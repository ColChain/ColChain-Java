package org.colchain.colchain.sparql.iter;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.NiceIterator;

import java.util.NoSuchElementException;

public class EmptyIterator extends NiceIterator<Triple> {
    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Triple next() {
        throw new NoSuchElementException();
    }
}
