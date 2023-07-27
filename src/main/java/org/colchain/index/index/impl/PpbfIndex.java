package org.colchain.index.index.impl;

import org.colchain.index.graph.IGraph;
import org.colchain.index.index.IndexBase;
import org.colchain.index.index.IndexMapping;
import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;

import java.util.*;

public class PpbfIndex extends IndexBase {
    private Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = new HashMap<>();
    private Map<IGraph, IBloomFilter<String>> bs = new HashMap<>();
    private Set<IGraph> fragments = new HashSet<>();

    public PpbfIndex(Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms,
                     Map<IGraph, IBloomFilter<String>> bs, Set<IGraph> fragments) {
        this.blooms = blooms;
        this.bs = bs;
        this.fragments = fragments;
    }

    public PpbfIndex() {
    }

    public void addFragment(IGraph graph, IBloomFilter<String> filter) {
        fragments.add(graph);
        bs.put(graph, filter);
    }

    public Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> getBlooms() {
        return blooms;
    }

    public Map<IGraph, IBloomFilter<String>> getBs() {
        return bs;
    }

    @Override
    public Set<IGraph> getRelevantFragments(List<Triple> triples) {
        Set<IGraph> fragments = new HashSet<>();

        for (Triple triple : triples) {
            fragments.addAll(getFragmentsByPredicate(triple.getPredicate()));
        }

        return fragments;
    }

    @Override
    public List<String> getByPredicate(String predicate) {
        List<String> ids = new ArrayList<>();

        for (IGraph g : fragments) {
            if (g.getBaseUri().equals(predicate))
                ids.add(g.getId());
        }

        return ids;
    }

    private List<IGraph> getFragmentsByPredicate(String predicate) {
        List<IGraph> ids = new ArrayList<>();

        for (IGraph g : fragments) {
            if (g.getBaseUri().equals(predicate))
                ids.add(g);
        }

        return ids;
    }

    @Override
    public Set<IGraph> getGraphs() {
        return fragments;
    }

    @Override
    public void updateIndex(String fragmentId, IBloomFilter<String> filter) {
        Set<IGraph> ks = bs.keySet();
        for (IGraph g : ks) {
            if (g.getId().equals(fragmentId)) bs.put(g, filter);
        }

        Set<Tuple<IGraph, IGraph>> kks = blooms.keySet();
        for (Tuple<IGraph, IGraph> g : kks) {
            if (g.getFirst().getId().equals(fragmentId) || g.getSecond().getId().equals(fragmentId)) {
                blooms.get(g).deleteFile();
                blooms.remove(g);
            }
        }
    }

    @Override
    public boolean hasFragment(String fid) {
        for (IGraph g : fragments) {
            if (g.getId().equals(fid)) return true;
        }
        return false;
    }

    @Override
    public void removeCommunity(String id) {
        System.out.println("removing indexes");
        Set<IGraph> keys = new HashSet<>(bs.keySet());

        for (IGraph key : keys) {
            if (key.isCommunity(id)) {
                IBloomFilter<String> f = bs.get(key);
                f.deleteFile();

                bs.remove(key);
                fragments.remove(key);
            }
        }

        Set<Tuple<IGraph, IGraph>> ks = new HashSet<>(blooms.keySet());
        for (Tuple<IGraph, IGraph> k : ks) {
            if (k.getFirst().isCommunity(id) || k.getSecond().isCommunity(id)) {
                IBloomFilter<String> f = blooms.get(k);
                f.deleteFile();
                blooms.remove(k);
            }
        }
    }

    @Override
    public boolean isBuilt() {
        return bs.size() > 0;
    }

    private String bound(Triple t1, Triple t2) {
        if (t1.getSubject().equals(t2.getSubject()) || t1.getSubject().equals(t2.getObject())) return t1.getSubject();
        else if (t1.getObject().equals(t2.getSubject()) || t1.getObject().equals(t2.getObject())) return t1.getObject();
        return null;
    }

    private boolean isVar(String e) {
        return e.equals("ANY") || e.startsWith("?");
    }

    String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
    @Override
    public IndexMapping getMapping(List<Triple> query) {
        /*Map<Triple, Set<IGraph>> identifiable = new HashMap<>();
        for (Triple t : query) {
            identifiable.put(t, new HashSet<>());
        }

        for (IGraph f : fragments) {
            for (Triple t : query) {
                if (f.identify(t)) {
                    if (t.getSubject().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getSubject())) {
                            continue;
                        }
                    }
                    if (t.getPredicate().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getPredicate())) {
                            continue;
                        }
                    }
                    if (t.getObject().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getObject())) {
                            continue;
                        }
                    }

                    identifiable.get(t).add(f);
                }

            }
        }

        Map<Tuple<Triple, Triple>, String> bindings = new HashMap<>();
        for (Triple t1 : query) {
            for (Triple t2 : query) {
                if (t1.equals(t2) || bindings.containsKey(new Tuple<>(t1, t2)) || bindings.containsKey(new Tuple<>(t2, t1)))
                    continue;
                String b = bound(t1, t2);
                if (b != null && b.startsWith("?")) {
                    bindings.put(new Tuple<>(t1, t2), b);
                }
            }
        }

        for (Map.Entry<Tuple<Triple, Triple>, String> binding : bindings.entrySet()) {
            Triple t1 = binding.getKey().getFirst();
            Triple t2 = binding.getKey().getSecond();

            Set<IGraph> f1s = identifiable.get(t1);
            Set<IGraph> f2s = identifiable.get(t2);

            Set<IGraph> good1 = new HashSet<>(f1s.size());
            Set<IGraph> good2 = new HashSet<>(f2s.size());

            for (IGraph f1 : f1s) {
                for (IGraph f2 : f2s) {
                    if (f1.equals(f2)) {
                        good1.add(f1);
                        good2.add(f2);
                        continue;
                    }
                    Tuple<IGraph, IGraph> tuple = new Tuple<>(f1, f2);
                    if (!blooms.containsKey(tuple)) tuple = new Tuple<>(f2, f1);
                    if (blooms.containsKey(tuple)) {
                        IBloomFilter<String> intersection = blooms.get(tuple);
                        if (!intersection.isEmpty()) {
                            good1.add(f1);
                            good2.add(f2);
                        }
                    } else {
                        IBloomFilter<String> intersection = bs.get(f1).intersect(bs.get(f2));
                        blooms.put(tuple, intersection);
                        if (!intersection.isEmpty()) {
                            good1.add(f1);
                            good2.add(f2);
                        }
                    }
                }
            }

            if (good1.size() < f1s.size()) {
                //          change = true;
                identifiable.put(t1, good1);
            }
            if (good2.size() < f2s.size()) {
                //          change = true;
                identifiable.put(t2, good2);
            }
        }
        //}

        return new IndexMapping(identifiable);*/

        return getMappingOpt(query);
    }

    private IndexMapping getMappingOpt(List<Triple> query) {
        Map<Triple, Set<IGraph>> identifiable = new HashMap<>();

        for (Triple t : query) {
            identifiable.put(t, new HashSet<>());
        }

        String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        for (IGraph f : fragments) {
            for (Triple t : query) {
                if (f.identify(t)) {
                    if (t.getSubject().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getSubject())) {
                            continue;
                        }
                    }
                    if (t.getPredicate().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getPredicate())) {
                            continue;
                        }
                    }
                    if (t.getObject().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getObject())) {
                            continue;
                        }
                    }

                    identifiable.get(t).add(f);
                }

            }
        }

        Map<Tuple<Triple, Triple>, String> bindings = new HashMap<>();
        for (Triple t1 : query) {
            for (Triple t2 : query) {
                if (t1.equals(t2) || bindings.containsKey(new Tuple<>(t1, t2)) || bindings.containsKey(new Tuple<>(t2, t1)))
                    continue;
                String b = bound(t1, t2);
                if (b != null && b.startsWith("?")) {
                    bindings.put(new Tuple<>(t1, t2), b);
                }
            }
        }

        //boolean change = true;
        //while (change) {
        //    change = false;

            List<Triple> triples = new ArrayList<>(query);
            Set<Tuple<Triple, Triple>> checked = new HashSet<>();

            while (triples.size() > 0) {
                Triple curr = getSmallest(triples, identifiable);
                triples.remove(curr);

                for (Map.Entry<Tuple<Triple, Triple>, String> binding : bindings.entrySet()) {
                    Triple t1 = binding.getKey().getFirst();
                    Triple t2 = binding.getKey().getSecond();

                    if(!t1.equals(curr) && !t2.equals(curr)) continue;
                    if(checked.contains(binding.getKey())) continue;
                    else checked.add(binding.getKey());

                    Set<IGraph> f1s = identifiable.get(t1);
                    Set<IGraph> f2s = identifiable.get(t2);

                    Set<IGraph> good1 = new HashSet<>(f1s.size());
                    Set<IGraph> good2 = new HashSet<>(f2s.size());

                    for (IGraph f1 : f1s) {
                        for (IGraph f2 : f2s) {
                            if (f1.equals(f2)) {
                                good1.add(f1);
                                good2.add(f2);
                                continue;
                            }
                            Tuple<IGraph, IGraph> tuple = new Tuple<>(f1, f2);
                            if (!blooms.containsKey(tuple)) tuple = new Tuple<>(f2, f1);
                            if (blooms.containsKey(tuple)) {
                                IBloomFilter<String> intersection = blooms.get(tuple);
                                if (!intersection.isEmpty()) {
                                    good1.add(f1);
                                    good2.add(f2);
                                }
                            } else {
                                IBloomFilter<String> intersection = bs.get(f1).intersect(bs.get(f2));
                                blooms.put(tuple, intersection);
                                if (!intersection.isEmpty()) {
                                    good1.add(f1);
                                    good2.add(f2);
                                }
                            }
                        }
                    }

                    if (good1.size() < f1s.size()) {
                        //change = true;
                        identifiable.put(t1, good1);
                    }
                    if (good2.size() < f2s.size()) {
                        //change = true;
                        identifiable.put(t2, good2);
                    }
                }
            }
        //}

        return new IndexMapping(identifiable);
    }

    private Triple getSmallest(List<Triple> triples, Map<Triple, Set<IGraph>> identifiable) {
        Triple ret = triples.get(0);
        int cnt = identifiable.get(ret).size();
        for(int i = 1; i < triples.size(); i++) {
            Triple curr = triples.get(i);
            int cnt1 = identifiable.get(curr).size();

            if(cnt1 < cnt) {
                ret = curr;
                cnt = cnt1;
            }
        }
        return ret;
    }

    private Set<IGraph> getIdentifiable(Triple t) {
        Set<IGraph> gs = new HashSet<>();
        for(IGraph g : fragments) {
            if(g.identify(t)) gs.add(g);
        }
        return gs;
    }

    private Triple selectFirst(List<Triple> query) {
        Triple first = query.get(0);
        int lowest = val(first);

        for(int i = 1; i < query.size(); i++) {
            Triple tpl = query.get(i);
            int val = val(tpl);
            if(val < lowest) {
                first = tpl;
                lowest = val;
            }
        }

        return first;
    }

    private int val(Triple triple) {
        if(triple.getSubject().startsWith("?")) {
            if(triple.getPredicate().startsWith("?")) {
                if(triple.getObject().startsWith("?"))
                    return 10000;
                else
                    return 200;
            } else {
                if(triple.getObject().startsWith("?"))
                    return 2000;
                else
                    return 5;
            }
        } else {
            if(triple.getPredicate().startsWith("?")) {
                if(triple.getObject().startsWith("?"))
                    return 40;
                else
                    return 10;
            } else {
                if(triple.getObject().startsWith("?"))
                    return 5;
                else
                    return 0;
            }
        }
    }

    @Override
    public IGraph getGraph(String id) {
        for (IGraph graph : fragments) {
            if (graph.getId().equals(id)) return graph;
        }
        return null;
    }

    @Override
    public String getPredicate(String id) {
        for (IGraph graph : fragments) {
            if (graph.getId().equals(id)) return graph.getBaseUri();
        }
        return "";
    }

    @Override
    public String toString() {
        return "PpbfIndex{" +
                "bs=" + bs +
                '}';
    }
}
