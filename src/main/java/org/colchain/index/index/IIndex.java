package org.colchain.index.index;

import org.colchain.index.graph.IGraph;
import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.util.Triple;

import java.util.List;
import java.util.Set;

public interface IIndex {
    boolean isBuilt();
    IndexMapping getMapping(List<Triple> query);
    void addFragment(IGraph graph, IBloomFilter<String> filter);
    void removeCommunity(String id);
    Set<IGraph> getGraphs();
    IGraph getGraph(String id);
    boolean hasFragment(String fid);
    void updateIndex(String fragmentId, IBloomFilter<String> filter);
    String getPredicate(String id);
    List<String> getByPredicate(String predicate);
}
