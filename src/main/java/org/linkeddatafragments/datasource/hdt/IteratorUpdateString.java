package org.linkeddatafragments.datasource.hdt;

import org.colchain.index.util.Triple;
import org.colchain.colchain.transaction.ITransaction;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.Queue;

public class IteratorUpdateString implements IteratorTripleString {
    private final IteratorTripleString it;
    private final Queue<Triple> triples;
    private final ITransaction transaction;
    private boolean isAtStart = true;
    private TripleString next = null;
    private long start = 0;

    public IteratorUpdateString(IteratorTripleString it, Queue<Triple> triples, ITransaction transaction) {
        this.it = it;
        this.triples = triples;
        this.transaction = transaction;
    }

    @Override
    public void goToStart() {
        isAtStart = true;
        it.goToStart();
    }

    @Override
    public long estimatedNumResults() {
        return it.estimatedNumResults() + triples.size();
    }

    @Override
    public ResultEstimationType numResultEstimation() {
        return it.numResultEstimation();
    }

    private void bufferNext() {
        if(it.hasNext()) {
            next = it.next();
            Triple t = new Triple(next.getSubject().toString(), next.getPredicate().toString(), next.getObject().toString());
            if(transaction.isDeleted(t)) {
                next = null;
                bufferNext();
            }
            return;
        }
        if(start == 0) start = System.currentTimeMillis();
        if(triples.size() == 0) {
            System.out.println(System.currentTimeMillis()-start);
            return;
        }
        Triple t = triples.poll();
        next = new TripleString(t.getSubject(), t.getPredicate(), t.getObject());
    }

    @Override
    public boolean hasNext() {
        if(next == null) bufferNext();
        return next != null;
    }

    @Override
    public TripleString next() {
        TripleString t = next;
        next = null;
        return t;
    }
}
