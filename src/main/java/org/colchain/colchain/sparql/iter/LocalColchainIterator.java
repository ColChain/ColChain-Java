package org.colchain.colchain.sparql.iter;

import org.colchain.colchain.sparql.ColchainBindings;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.util.iterator.NiceIterator;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.Iterator;

public class LocalColchainIterator extends NiceIterator<Pair<Triple, Binding>> {
    private Triple triple = null;
    private final HDT datasource;
    private final IteratorTriple it;
    private IteratorTripleString iterator = null;
    private Pair<Triple, Binding> next = null;

    public LocalColchainIterator(Triple triple, ColchainBindings bindings, HDT datasource) {
        //System.out.println("Local: " + triple.toString());
        this.datasource = datasource;
        this.it = new IteratorTriple(triple, bindings);
        if(it.hasNext()) {
            this.triple = it.next();
            try {
                iterator = datasource.search(
                        this.triple.getSubject().isVariable() ? "" : this.triple.getSubject().getURI(),
                        this.triple.getPredicate().isVariable() ? "" : this.triple.getPredicate().getURI(),
                        this.triple.getObject().isVariable() ? "" : (this.triple.getObject().isLiteral()? this.triple.getObject().getLiteral().getValue().toString() : this.triple.getObject().getURI()));
            } catch (NotFoundException e) {}
        }
    }

    private void bufferNext() {
        next = null;
        if (iterator == null) return;
        if (iterator.hasNext()) {
            TripleString nxt = iterator.next();
            Triple t = new Triple(
                    getNode(nxt.getSubject().toString()),
                    getNode(nxt.getPredicate().toString()),
                    getNode(nxt.getObject().toString()));

            Binding b = it.getCurrBinding();
            if(b == null) b = BindingFactory.create();
            /*if(triple.getSubject().isVariable())
                b.add((Var)triple.getSubject(), t.getSubject());
            if(triple.getPredicate().isVariable())
                b.add((Var)triple.getPredicate(), t.getPredicate());
            if(triple.getObject().isVariable())
                b.add((Var)triple.getObject(), t.getObject());*/
            next = new Pair<>(t, b);
            return;
        }

        if(it.hasNext()) {
            this.triple = it.next();
            try {
                iterator = datasource.search(
                        this.triple.getSubject().isVariable() ? "" : this.triple.getSubject().getURI(),
                        this.triple.getPredicate().isVariable() ? "" : this.triple.getPredicate().getURI(),
                        this.triple.getObject().isVariable() ? "" : (this.triple.getObject().isLiteral()? this.triple.getObject().getLiteral().getValue().toString() : this.triple.getObject().getURI()));
            } catch (NotFoundException e) {}
            bufferNext();
        }
    }

    @Override
    public boolean hasNext() {
        if(next == null) bufferNext();
        return next != null;
    }

    @Override
    public Pair<Triple, Binding> next() {
        Pair<Triple, Binding> p = next;
        next = null;
        return p;
    }

    private Node getNode(String element) {
        if (element.length() == 0) return NodeFactory.createBlankNode();
        char firstChar = element.charAt(0);
        if (firstChar == '_') {
            return NodeFactory.createBlankNode(element);
        } else if (firstChar == '"') {
            String noq = element.replace("\"", "");
            if (noq.matches("-?\\d+")) {
                return Util.makeIntNode(Integer.parseInt(noq));
            } else if (noq.matches("([0-9]+)\\.([0-9]+)")) {
                return Util.makeDoubleNode(Double.parseDouble(noq));
            }
            return NodeFactory.createLiteral(element.replace("\"", ""));
        } else {
            return NodeFactory.createURI(element);
        }
    }

    private class IteratorTriple implements Iterator<Triple> {
        private final Triple triple;
        private final ColchainBindings bindings;
        private int currId = 0;
        private Triple next = null;
        private Binding currBinding = null;

        public IteratorTriple(Triple triple, ColchainBindings bindings) {
            this.triple = triple;
            this.bindings = bindings;
        }

        private void bufferNext() {
            next = null;
            if(currId == 0 && bindings.get(0).isEmpty()) {
                next = triple;
                currId++;
                return;
            }

            if(currId >= bindings.size()) return;
            Binding b = bindings.get(currId);
            currBinding = b;
            currId++;

            next = new Triple(
                    (triple.getSubject().isVariable() && b.contains((Var) triple.getSubject()))? b.get((Var) triple.getSubject()) : triple.getSubject(),
                    (triple.getPredicate().isVariable() && b.contains((Var) triple.getPredicate()))? b.get((Var) triple.getPredicate()) : triple.getPredicate(),
                    (triple.getObject().isVariable() && b.contains((Var) triple.getObject()))? b.get((Var) triple.getObject()) : triple.getObject()
            );
        }

        @Override
        public boolean hasNext() {
            if(next == null) bufferNext();
            return next != null;
        }

        @Override
        public Triple next() {
            Triple t = next;
            next = null;
            return t;
        }

        public Binding getCurrBinding() {
            return currBinding;
        }
    }
}
