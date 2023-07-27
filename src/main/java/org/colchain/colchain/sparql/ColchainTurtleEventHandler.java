package org.colchain.colchain.sparql;

import org.apache.jena.graph.Triple;
import org.apache.jena.n3.turtle.TurtleEventHandler;

import java.util.*;

public class ColchainTurtleEventHandler implements TurtleEventHandler {
    private final String fragmentUrl;
    private Map<String, String> prefix = new HashMap<>();
    private List<Triple> triples = new ArrayList<>();
    private Set<Triple> processedTriples = new HashSet<>();
    private String nextPageUrl = "";
    private int numResults = 0;

    private static final int HYDRA_TOTALITEMS_HASH =
            new String("http://www.w3.org/ns/hydra/core#totalItems").hashCode();
    private static final int HYDRA_NEXTPAGE_HASH =
            new String("http://www.w3.org/ns/hydra/core#nextPage").hashCode();
    private static final int DATASET_HASH = new String("http://rdfs.org/ns/void#Dataset").hashCode();
    private static final int SUBSET_HASH = new String("http://rdfs.org/ns/void#subset").hashCode();

    public ColchainTurtleEventHandler(String fragmentUrl) {
        this.fragmentUrl = fragmentUrl;
    }

    @Override
    public void triple(int i, int i1, Triple triple) {
        if (processedTriples.contains(triple)) return;
        processedTriples.add(triple);

        if (isTripleValid(triple))
            triples.add(triple);
    }

    public boolean hasNextPage() {
        return !nextPageUrl.equals("");
    }

    public String getNextPageUrl() {
        return nextPageUrl;
    }

    public List<Triple> getTriples() {
        return triples;
    }

    public int getNumResults() {
        return numResults;
    }

    private boolean isTripleValid(Triple triple) {
        if (triple.getSubject().isURI() && triple.getSubject().getURI().equals(fragmentUrl)) {
            if (triple.getPredicate().getURI().hashCode() == HYDRA_NEXTPAGE_HASH) {
                nextPageUrl = triple.getObject().getURI();
            }
            if (triple.getPredicate().getURI().hashCode() == HYDRA_TOTALITEMS_HASH) {
                numResults = Integer.parseInt(triple.getObject().getLiteralValue().toString());
            }
            return false;
        } else if (triple.getPredicate().getURI().contains("hydra/")
                || (triple.getObject().isURI() && triple.getObject().getURI().contains("hydra/"))
                || (triple.getObject().isURI() && triple.getObject().getURI().hashCode() == DATASET_HASH)
                || triple.getPredicate().getURI().hashCode() == SUBSET_HASH) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void prefix(int i, int i1, String s, String s1) {
        prefix.put(s, s1);
    }

    @Override
    public void startFormula(int i, int i1) {
        // No idea
    }

    @Override
    public void endFormula(int i, int i1) {
        // No idea
    }
}
