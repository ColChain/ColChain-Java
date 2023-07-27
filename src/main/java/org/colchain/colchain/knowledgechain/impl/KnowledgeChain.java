package org.colchain.colchain.knowledgechain.impl;

import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.IKnowledgeChain;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.util.CryptoUtils;
import org.linkeddatafragments.datasource.IDataSource;

import java.io.IOException;
import java.security.PublicKey;
import java.util.*;

public class KnowledgeChain implements IKnowledgeChain {
    private ChainEntry entry;
    private transient IDataSource datasource;
    private final byte[] key;
    private Map<Long, IDataSource> versions = new HashMap<>();

    public KnowledgeChain(ChainEntry entry, IDataSource datasource, byte[] key) {
        this.entry = entry;
        this.datasource = datasource;
        this.key = key;
    }

    public KnowledgeChain(IDataSource dataSource, byte[] author) {
        entry = ChainEntry.getInitialEntry();
        this.datasource = dataSource;
        this.key = author;
    }

    public KnowledgeChain(byte[] author) {
        entry = ChainEntry.getInitialEntry();
        this.datasource = null;
        this.key = author;
    }

    public void setDatasource(IDataSource datasource) {
        this.datasource = datasource;
    }

    @Override
    public void remove() {
        try {
            datasource.close();
        } catch (IOException e) {
        }
        datasource.remove();
    }

    @Override
    public ChainEntry top() {
        return entry;
    }

    @Override
    public void transition(ITransaction transaction) {
        // Chain part
        transaction.setTimestamp();
        entry = new ChainEntry(transaction, entry);
        if (transaction.getAuthor().equals(AbstractNode.getState().getId())) {
            datasource.updateHdt(transaction);
            datasource.deleteBloomFilter();
            //AbstractNode.getState().getIndex().updateIndex(transaction.getFragmentId(), datasource.createBloomFilter());
            datasource.createBloomFilter();

        //if (transaction.getAuthor().equals(AbstractNode.getState().getId())) {
            Set<CommunityMember> parts = AbstractNode.getState().getCommunityByFragmentId(transaction.getFragmentId()).getParticipants();
            for (CommunityMember p : parts) {
                p.updateFragment(transaction.getFragmentId());
            }
        }
    }

    @Override
    public IDataSource getDatasource() {
        return datasource;
    }

    @Override
    public IDataSource getDatasource(long timestamp) {
        long ts = getTimestamp(timestamp);
        if(!versions.containsKey(ts)) {
            Tuple<Set<Triple>, Set<Triple>> tpl = getChanges(timestamp);
            versions.put(ts, datasource.materializeVersion(tpl.getFirst(), tpl.getSecond(), ts));
        }
        return versions.get(ts);
    }

    private long getTimestamp(long timestamp) {
        ChainEntry entry = this.entry;

        while (!entry.isFirst() && timestamp < entry.getTransaction().getTimestamp()) {
            if(entry.isFirst()) break;
            entry = entry.previous();
        }

        if(entry.isFirst()) return 0;
        return entry.getTransaction().getTimestamp();
    }

    private Tuple<Set<Triple>, Set<Triple>> getChanges(long timestamp) {
        ChainEntry entry = this.entry;
        Set<Triple> additions = new HashSet<>();
        Set<Triple> deletions = new HashSet<>();

        while (!entry.isFirst() && timestamp < entry.getTransaction().getTimestamp()) {
            if(entry.isFirst()) break;
            List<Operation> operations = entry.getTransaction().getOperations();

            for(Operation op : operations) {
                Triple tpl = op.getTriple();
                if(op.getType() == Operation.OperationType.ADD) {
                    if(deletions.contains(tpl)) deletions.remove(tpl);
                    else additions.add(tpl);
                } else {
                    if(additions.contains(tpl)) additions.remove(tpl);
                    else deletions.add(tpl);
                }
            }

            entry = entry.previous();
        }

        return new Tuple<>(additions, deletions);
    }

    @Override
    public PublicKey getKey() {
        return CryptoUtils.getPublicKey(key);
    }

    @Override
    public void copy() {
        datasource.copy();
    }

    @Override
    public void restore() {
        datasource.restore();
    }

    @Override
    public long size() {
        return datasource.size();
    }
}
