package org.colchain.colchain.sparql.graph;

import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;

public class ColchainStatistics implements GraphStatisticsHandler {
    private final ColchainGraph graph;

    ColchainStatistics(ColchainGraph graph) {
        this.graph = graph;
    }

    @Override
    public long getStatistic(Node subject, Node predicate, Node object) {
        return graph.graphBaseSize();
    }
}
