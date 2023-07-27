package org.colchain.index.graph;

import org.colchain.index.util.Triple;

public interface IGraph {
    boolean identify(Triple triplePattern);
    boolean isCommunity(String id);
    String getCommunity();
    String getId();
    String getBaseUri();
}
