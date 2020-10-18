package org.colchain.index.graph.impl;

import org.colchain.index.graph.GraphBase;
import org.colchain.index.util.Triple;
import com.google.gson.Gson;

public class Graph extends GraphBase {
    public Graph(String community, String baseUri, String id) {
        super(community, baseUri, id);
    }

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getBaseUri());
    }

    @Override
    public int hashCode() {
        return getBaseUri().hashCode() + getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != getClass()) return false;
        Graph other = (Graph) obj;
        return getBaseUri().equals(other.getBaseUri()) && getId().equals(other.getId());
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
