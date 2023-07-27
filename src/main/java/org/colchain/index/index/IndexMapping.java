package org.colchain.index.index;

import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.node.AbstractNode;
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

    public int getNumFragments() {
        Set<IGraph> graphs = new HashSet<>();
        for(Set<IGraph> fragments : mapping.values()) {
            graphs.addAll(fragments);
        }
        return graphs.size();
    }

    public int getNumNodes() {
        Set<IGraph> graphs = new HashSet<>();
        for(Set<IGraph> fragments : mapping.values()) {
            graphs.addAll(fragments);
        }
        Set<CommunityMember> set = new HashSet<>();
        for(IGraph g : graphs) {
            set.addAll(AbstractNode.getState().getCommunityByFragmentId(g.getId()).getParticipants());
        }

        return set.size();
    }

    public int getNumNodesLocal(){
        int num = 0;
        Set<IGraph> graphs = new HashSet<>();
        for(Set<IGraph> fragments : mapping.values()) {
            graphs.addAll(fragments);
        }

        for(IGraph fragment : graphs) {
            if(AbstractNode.getState().getCommunityByFragmentId(fragment.getId()).getMemberType() == Community.MemberType.PARTICIPANT)
                num++;
        }
        return num;
    }

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

    public Set<CommunityMember> getInvolvedNodes() {
        Set<CommunityMember> nodes = new HashSet<>();
        for(Triple key : mapping.keySet()) {
            for(IGraph fragment : mapping.get(key)) {
                nodes.add(AbstractNode.getState().getCommunityByFragmentId(fragment.getId()).getParticipant());
            }
        }
        return nodes;
    }

    @Override
    public String toString() {
        return "IndexMapping{" +
                "mapping=" + mapping +
                '}';
    }
}
