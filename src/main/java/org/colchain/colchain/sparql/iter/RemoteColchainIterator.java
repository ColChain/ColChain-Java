package org.colchain.colchain.sparql.iter;

import org.colchain.colchain.sparql.ColchainBindings;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.colchain.colchain.sparql.ColchainTurtleEventHandler;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.n3.turtle.parser.ParseException;
import org.apache.jena.n3.turtle.parser.TurtleParser;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.util.iterator.NiceIterator;

import java.io.IOException;
import java.util.*;

public class RemoteColchainIterator extends NiceIterator<Pair<Triple, Binding>> {
    private String currUrl = null;
    private final Triple triple;
    private final ColchainBindings bindings;
    private Queue<Pair<Triple, Binding>> results = new LinkedList<>();
    private Pair<Triple, Binding> next = null;

    public RemoteColchainIterator(String startUrl, Triple triple, ColchainBindings bindings) {
        //System.out.println("Remote: " + triple.toString() + " \nStartURL: " + startUrl);
        this.currUrl = startUrl;
        this.triple = triple;
        this.bindings = bindings;
    }

    private void bufferNext() {
        if(results.size() == 0) {
            if(currUrl == null) return;
            parseNext();
            bufferNext();
            return;
        }

        next = results.poll();
    }

    @Override
    public boolean hasNext() {
        if(next == null) bufferNext();
        return next != null;
    }

    @Override
    public Pair<Triple, Binding> next() {
        Pair<Triple, Binding> r = next;
        next = null;
        return r;
    }

    private void parseNext() {
        if(currUrl == null) return;
        Content content = null;
        try {
            ColchainJenaConstants.NEM++;
            content = Request.Get(currUrl).addHeader("accept", "text/turtle").execute().returnContent();
            ColchainJenaConstants.NTB += content.asBytes().length;
        } catch (IOException e) {
            currUrl = null;
            return;
        }
        //System.out.println(content.asString());
        TurtleParser parser = new TurtleParser(content.asStream());
        ColchainTurtleEventHandler handler = new ColchainTurtleEventHandler(currUrl);
        parser.setEventHandler(handler);
        try {
            parser.parse();
        } catch (ParseException e) {
            e.printStackTrace();
            currUrl = null;
            return;
        }

        List<Triple> lst = handler.getTriples();
        //System.out.println("Results: " + lst.toString());
        if(handler.hasNextPage()) currUrl = handler.getNextPageUrl();
        else currUrl = null;
        results = extendBindings(bindings, triple, lst);
    }

    private Queue<Pair<Triple, Binding>> extendBindings(ColchainBindings bindings, Triple tp, List<Triple> triples) {
        Queue<Pair<Triple, Binding>> extendedBindings = new LinkedList<>();
        if(bindings.size() == 0) {
            return new LinkedList<>();
        }
        if (bindings.get(0).isEmpty()) {
            for (Triple triple : triples) {
                Binding b = bindings.get(0);
                /*if(tp.getSubject().isVariable())
                    b.add((Var)tp.getSubject(), triple.getSubject());
                if(tp.getPredicate().isVariable())
                    b.add((Var)tp.getPredicate(), triple.getPredicate());
                if(tp.getObject().isVariable())
                    b.add((Var)tp.getObject(), triple.getObject());*/
                extendedBindings.add(new Pair<>(triple, b));
            }
        } else {
            int size = bindings.size();
            for (int i = 0; i < size; i++) {
                Binding currentBinding = bindings.get(i);
                for (Triple triple : triples) {
                    if (matchesWithBinding(tp, triple, currentBinding)) {
                        Binding b = currentBinding;
                        /*if(tp.getSubject().isVariable())
                            b.add((Var)tp.getSubject(), triple.getSubject());
                        if(tp.getPredicate().isVariable())
                            b.add((Var)tp.getPredicate(), triple.getPredicate());
                        if(tp.getObject().isVariable())
                            b.add((Var)tp.getObject(), triple.getObject());*/
                        extendedBindings.add(new Pair<>(triple, b));
                    }
                }
            }

        }
        return extendedBindings;
    }

    private boolean matchesWithBinding(Triple tp, Triple triple, Binding binding) {
        if(tp.getSubject().isVariable() && binding.contains((Var) tp.getSubject())) {
            if(!binding.get((Var)tp.getSubject()).getURI().equals(triple.getSubject().getURI()))
                return false;
        }

        if(tp.getPredicate().isVariable() && binding.contains((Var) tp.getPredicate())) {
            if(!binding.get((Var)tp.getPredicate()).getURI().equals(triple.getPredicate().getURI()))
                return false;
        }

        if(tp.getObject().isVariable() && binding.contains((Var) tp.getObject())) {
            String val1 = "";
            if(triple.getObject().isLiteral()) val1 = triple.getObject().getLiteral().toString();
            else val1 = triple.getObject().getURI();

            Node n = binding.get((Var)tp.getObject());
            String val2 = "";
            if(n.isLiteral()) val2 = n.getLiteral().toString();
            else val2 = n.getURI();

            if(!val1.equals(val2))
                return false;
        }

        return true;

    }

}
