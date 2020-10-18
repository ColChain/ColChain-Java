package org.colchain.colchain.knowledgechain;

import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.transaction.ITransaction;
import org.linkeddatafragments.datasource.IDataSource;

import java.security.PublicKey;

public interface IKnowledgeChain {
    IDataSource getDatasource();
    void transition(ITransaction transaction);
    ChainEntry top();
    void remove();
    PublicKey getKey();
    void copy();
    long size();
    void restore();
    IDataSource getDatasource(long timestamp);
}
