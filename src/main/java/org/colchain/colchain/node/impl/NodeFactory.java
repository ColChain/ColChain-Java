package org.colchain.colchain.node.impl;

import org.colchain.index.index.IIndex;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.node.INode;
import org.colchain.colchain.transaction.ITransaction;

import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NodeFactory {
    public static INode create(String id, String datastore, String address, Map<String, KnowledgeChain> chains, List<Community> communities,
                               KeyPair keys, IIndex index, Map<String, Tuple<ITransaction, Set<String>>> transactions) {
        return new NodeImpl(id, datastore, address, chains, communities, keys, index, transactions);
    }
}
