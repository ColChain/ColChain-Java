package org.colchain.index.index;

import org.colchain.index.graph.IGraph;
import org.colchain.index.util.Triple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexMapping {
    private Map<Triple, Set<IGraph>> mapping = new HashMap<>();

    public IndexMapping(Map<Triple, Set<IGraph>> mapping) {
        this.mapping = mapping;
    }

    public IndexMapping() {}

    public void addMapping(Triple triple, IGraph graph) {
        if(mapping.containsKey(triple)) {
            mapping.get(triple).add(graph);
            return;
        }
        Set<IGraph> set = new HashSet<>();
        set.add(graph);
        mapping.put(triple, set);
    }

    public Set<IGraph> getMapping(Triple triple){
        return mapping.get(triple);
    }

    @Override
    public String toString() {
        return "IndexMapping{" +
                "mapping=" + mapping +
                '}';
    }
}
