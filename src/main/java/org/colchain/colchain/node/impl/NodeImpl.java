package org.colchain.colchain.node.impl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.colchain.index.graph.IGraph;
import org.colchain.index.graph.impl.Graph;
import org.colchain.index.index.IIndex;
import org.colchain.index.index.impl.PpbfIndex;
import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.node.INode;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.util.CryptoUtils;
import org.colchain.colchain.util.NodeSerializer;
import org.colchain.colchain.util.RandomString;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.IDataSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;

public class NodeImpl extends AbstractNode implements INode {
    private Map<String, KnowledgeChain> chains = new HashMap<>();
    private String id;
    private List<Community> communities = new ArrayList<>();
    private KeyPair keys;
    private IIndex index = new PpbfIndex();
    private Map<String, Tuple<ITransaction, Set<String>>> transactions = new HashMap<>();

    public NodeImpl(String id, String datastore, String address, Map<String, KnowledgeChain> chains, List<Community> communities,
             KeyPair keys, IIndex index, Map<String, Tuple<ITransaction, Set<String>>> transactions) {
        this.chains = chains;
        this.id = id;
        this.communities = communities;
        this.keys = keys;
        this.index = index;
        this.transactions = transactions;
        setDatastore(datastore);
        setAddress(address);
    }

    public NodeImpl() {
        RandomString gen = new RandomString();
        id = gen.nextString();
        keys = CryptoUtils.generateKeyPair();
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public KeyPair getKeys() {
        return keys;
    }

    @Override
    public Map<String, Tuple<ITransaction, Set<String>>> getTransactions() {
        return transactions;
    }

    @Override
    public Map<String, KnowledgeChain> getChains() {
        return chains;
    }

    @Override
    public List<ITransaction> getPendingUpdates() {
        List<ITransaction> ret = new ArrayList<>();

        for(String s : transactions.keySet()) {
            Tuple<ITransaction, Set<String>> tpl = transactions.get(s);
            if(!tpl.getSecond().contains(id)) ret.add(tpl.getFirst());
        }

        return ret;
    }

    @Override
    public void addPending(Map<String, Tuple<ITransaction, Set<String>>> pending) {
        transactions.putAll(pending);
    }

    @Override
    public void suggestTransaction(ITransaction t, byte[] signature) {
        Set<String> acc = new HashSet<>();
        transactions.put(t.getId(), new Tuple<>(t,acc));

        acceptTransaction(t.getId(), t.getAuthor());
        if(!transactions.containsKey(t.getId())) return;
        PublicKey key = chains.get(t.getFragmentId()).getKey();
        if(key == null) return;
        if(CryptoUtils.matches(key, t, signature)) {
            acceptLocally(t);
        }
    }

    private void acceptLocally(ITransaction transaction) {
        String fid = transaction.getFragmentId();

        chains.get(fid).transition(transaction);
        transactions.remove(transaction.getId());
    }

    @Override
    public void accept(ITransaction t) {
        acceptTransaction(t.getId(), AbstractNode.getState().getId());
        Community c = getCommunityByFragmentId(t.getFragmentId());
        Set<CommunityMember> parts = c.getParticipants();

        for (CommunityMember p : parts) {
            if(p.getAddress().equals(AbstractNode.getState().getAddress())) continue;
            p.accept(t.getId());
        }
    }

    @Override
    public ITransaction getSuggestedTransaction(String id) {
        return transactions.get(id).getFirst();
    }

    @Override
    public void acceptTransaction(String tid, String nid) {
        Tuple<ITransaction, Set<String>> tpl = transactions.get(tid);
        String fid = tpl.getFirst().getFragmentId();
        Community c = getCommunityByFragmentId(fid);
        if(c == null || !c.containsParticipant(nid)) return;

        tpl.getSecond().add(nid);
        int needed = (int)Math.ceil(((double)c.getParticipants().size()) / 2.0);
        if(tpl.getSecond().size() >= needed) {
            chains.get(fid).transition(tpl.getFirst());
            transactions.remove(tid);
        }
    }

    @Override
    public Community getCommunityByFragmentId(String id) {
        for(Community c : communities) {
            if(c.isIn(id)) return c;
        }
        return null;
    }

    @Override
    public PrivateKey getPrivateKey() {
        return keys.getPrivate();
    }

    @Override
    public void leaveCommunity(String id) {
        Community comm = null;
        for(Community c : communities) {
            if(c.getId().equals(id)) {
                communities.remove(c);
                comm = c;
                break;
            }
        }

        if(comm == null) return;
        Set<String> fids = comm.getFragmentIds();
        for(String fid : fids) {
            KnowledgeChain chain = chains.get(fid);
            if(chain == null) break;
            chain.remove();
            chains.remove(fid);
        }

        index.removeCommunity(id);
    }

    @Override
    public KnowledgeChain getChain(String id) {
        return chains.get(id);
    }

    @Override
    public void addChain(String id, KnowledgeChain chain) {
        this.chains.put(id, chain);
    }

    @Override
    public void addCommunity(Community community) {
        communities.add(community);
    }

    @Override
    public void setKeyPair(KeyPair keys) {
        this.keys = keys;
    }

    @Override
    public boolean hasCommunity(String id) {
        for(Community c : communities) {
            if(c.getId().equals(id))
                return true;
        }

        return false;
    }

    @Override
    public Community getCommunity(String id) {
        for(Community c : communities) {
            if(c.getId().equals(id))
                return c;
        }

        return null;
    }

    @Override
    public void addParticipant(CommunityMember m, String id) {
        for(Community c : communities) {
            if(c.getId().equals(id))
                c.addParticipant(m);
        }
    }

    @Override
    public void addObserver(CommunityMember m, String id) {
        for(Community c : communities) {
            if(c.getId().equals(id))
                c.addObserver(m);
        }
    }

    @Override
    public void removeMember(CommunityMember m, String id) {
        for(Community c : communities) {
            if(c.getId().equals(id))
                c.removeMember(m);
        }
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public byte[] getPublicKey() {
        return keys.getPublic().getEncoded();
    }

    @Override
    public IDataSource getDatasource(String id) {
        if(!chains.containsKey(id))
            return null;
        return chains.get(id).getDatasource();
    }

    @Override
    public IDataSource getDatasource(String id, long timestamp) {
        if(!chains.containsKey(id))
            return null;
        return chains.get(id).getDatasource(timestamp);
    }

    @Override
    public HashMap<String, IDataSource> getDatasources() {
        HashMap<String, IDataSource> map = new HashMap<>();

        for(Map.Entry<String, KnowledgeChain> entry : chains.entrySet()) {
            map.put(entry.getKey(), entry.getValue().getDatasource());
        }

        return map;
    }

    @Override
    public IIndex getIndex() {
        return index;
    }

    @Override
    public List<Community> getCommunities() {
        return communities;
    }

    @Override
    public void setCommunities(List<Community> communities) {
        this.communities = communities;
    }

    @Override
    public Set<String> getDatasourceIds() {
        return chains.keySet();
    }

    @Override
    public void addNewFragment(String id, String predicate, String path, String community, byte[] key) {
        IDataSource dataSource;
        try {
            dataSource = DataSourceFactory.createLocal(id, path);
        } catch (IOException e) {
            return;
        }
        chains.put(id, new KnowledgeChain(dataSource, key));

        IGraph graph = new Graph(community, predicate, id);
        IBloomFilter<String> filter = dataSource.createBloomFilter();

        index.addFragment(graph, filter);

        for(Community c : communities) {
            if(c.getId().equals(community)) {
                c.addFragment(id);
            }
        }
    }

    @Override
    public void addNewFragment(String id, String predicate, String path, String community, byte[] key, ChainEntry entry) {
        IDataSource dataSource;
        try {
            dataSource = DataSourceFactory.createLocal(id, path);
        } catch (IOException e) {
            return;
        }
        chains.put(id, new KnowledgeChain(entry, dataSource, key));

        IGraph graph = new Graph(community, predicate, id);
        IBloomFilter<String> filter = dataSource.createBloomFilter();

        index.addFragment(graph, filter);

        for(Community c : communities) {
            if(c.getId().equals(community)) {
                c.addFragment(id);
            }
        }
    }

    @Override
    public void addNewObservedFragment(String id, String predicate, String community, byte[] key) {
        IGraph graph = new Graph(community, predicate, id);
        String datastore = getDatastore();
        datastore = datastore + (datastore.endsWith("/")? "" : "/");
        IBloomFilter<String> filter = PrefixPartitionedBloomFilter.create(datastore + "index/" + id + ".hdt.ppbf");

        index.addFragment(graph, filter);

        for(Community c : communities) {
            if(c.getId().equals(community)) {
                c.addFragment(id);
            }
        }
    }

    @Override
    public void saveState(String filename) {
        Gson gson = new GsonBuilder().registerTypeAdapter(INode.class, new NodeSerializer()).create();
        String json = gson.toJson(this, INode.class);

        try {
            PrintWriter writer = new PrintWriter(filename);
            writer.println(json);
            writer.flush();
            writer.close();
        } catch (IOException e) {}
    }
}
