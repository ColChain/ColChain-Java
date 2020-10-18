package org.linkeddatafragments.datasource.hdt;

import org.colchain.index.util.Triple;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.Queue;
import java.util.Set;

public class IteratorMaterializeString implements IteratorTripleString {
    private final IteratorTripleString it;
    private final Queue<Triple> triples;
    private final Set<Triple> deletions;
    private boolean isAtStart = true;
    private TripleString next = null;

    public IteratorMaterializeString(IteratorTripleString it, Queue<Triple> additions, Set<Triple> deletions) {
        this.it = it;
        this.triples = additions;
        this.deletions = deletions;
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
            if(deletions.contains(t)) {
                next = null;
                bufferNext();
            }
            return;
        }
        if(triples.size() == 0) {
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
