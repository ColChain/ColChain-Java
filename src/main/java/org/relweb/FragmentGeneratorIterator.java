package org.relweb;

import org.rdfhdt.hdt.triples.TripleString;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FragmentGeneratorIterator implements Iterator<TripleString> {
    private Queue<Iterator<TripleString>> queue = new LinkedList<>();
    private Iterator<TripleString> currIterator = null;
    private TripleString curr = null;

    public FragmentGeneratorIterator() {}
    public FragmentGeneratorIterator(Iterator<TripleString> it) {
        queue.add(it);
    }

    public void addIterator(Iterator<TripleString> it) {
        queue.add(it);
    }

    private void buffer() {
        if(curr != null) return;
        if(currIterator == null && queue.size() > 0) {
            currIterator = queue.poll();
        }

        if(currIterator != null) {
            if(!currIterator.hasNext()) {
                currIterator = null;
                buffer();
                return;
            }
            curr = currIterator.next();
        }
    }

    @Override
    public boolean hasNext() {
        if(curr == null) buffer();
        return curr != null;
    }

    @Override
    public TripleString next() {
        TripleString ts = curr;
        curr = null;
        return ts;
    }
}
