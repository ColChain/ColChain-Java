package org.colchain.colchain.util;

import com.google.gson.*;
import org.colchain.index.graph.IGraph;
import org.colchain.index.graph.impl.Graph;
import org.colchain.index.index.IIndex;
import org.colchain.index.index.impl.PpbfIndex;
import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.colchain.index.util.Triple;
import org.colchain.index.util.Tuple;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.knowledgechain.impl.ChainEntry;
import org.colchain.colchain.knowledgechain.impl.KnowledgeChain;
import org.colchain.colchain.node.INode;
import org.colchain.colchain.node.impl.NodeFactory;
import org.colchain.colchain.transaction.ITransaction;
import org.colchain.colchain.transaction.Operation;
import org.colchain.colchain.transaction.impl.TransactionFactory;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.hdt.HdtDataSource;

import java.io.IOException;
import java.lang.reflect.Type;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;

public class NodeSerializer implements JsonSerializer<INode>, JsonDeserializer<INode> {
    @Override
    public INode deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject obj = jsonElement.getAsJsonObject();

        String id = obj.get("id").getAsString();
        String datastore = obj.get("datastore").getAsString();
        String address = obj.get("address").getAsString();
        KeyPair keys = deserializeKeys(obj.get("keys"));
        Map<String, KnowledgeChain> chains = deserializeChains(obj.get("chains"));
        Map<String, Tuple<ITransaction, Set<String>>> transactions = deserializeTransactions(obj.get("transactions"));
        IIndex index = deserializeIndex(obj.get("index"));
        List<Community> communities = new ArrayList<>();

        JsonArray arr = obj.getAsJsonArray("communities");
        for(int i = 0; i < arr.size(); i++) {
            communities.add(deserializeCommunity(arr.get(i)));
        }

        return NodeFactory.create(id, datastore, address, chains, communities, keys, index, transactions);
    }

    @Override
    public JsonElement serialize(INode node, Type type, JsonSerializationContext jsonSerializationContext) {
        JsonObject obj = new JsonObject();
        obj.addProperty("id", node.getId());
        obj.addProperty("datastore", node.getDatastore());
        obj.addProperty("address", node.getAddress());
        obj.add("keys", serializeKeys(node.getKeys()));
        obj.add("chains", serializeChains(node.getChains()));

        JsonArray arr = new JsonArray();
        for(Community c : node.getCommunities()) {
            arr.add(serializeCommunity(c));
        }
        obj.add("communities", arr);

        obj.add("transactions", serializeTransactions(node.getTransactions()));
        obj.add("index", serializeIndex(node.getIndex()));

        return obj;
    }

    private JsonElement serializeIndex(IIndex index) {
        JsonObject obj = new JsonObject();

        JsonArray fArr = new JsonArray();
        Set<IGraph> graphs = index.getGraphs();
        for(IGraph graph : graphs) {
            fArr.add(serializeGraph(graph));
        }
        obj.add("fragments", fArr);

        JsonArray bArr = new JsonArray();
        Map<IGraph, IBloomFilter<String>> bs = ((PpbfIndex) index).getBs();
        for(IGraph graph : bs.keySet()) {
            JsonObject o = new JsonObject();
            o.add("graph", serializeGraph(graph));
            o.addProperty("filename", ((PrefixPartitionedBloomFilter)bs.get(graph)).getFilename());
            bArr.add(o);
        }
        obj.add("bs", bArr);

        JsonArray blArr = new JsonArray();
        Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = ((PpbfIndex) index).getBlooms();
        for(Tuple<IGraph, IGraph> gs : blooms.keySet()) {
            JsonObject o = new JsonObject();
            o.add("graph1", serializeGraph(gs.getFirst()));
            o.add("graph2", serializeGraph(gs.getSecond()));
            o.addProperty("filename", ((PrefixPartitionedBloomFilter)blooms.get(gs)).getFilename());
            blArr.add(o);
        }
        obj.add("blooms", blArr);

        return obj;
    }

    private IIndex deserializeIndex(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();

        Set<IGraph> graphs = new HashSet<>();
        JsonArray fArr = obj.getAsJsonArray("fragments");
        for(int i = 0; i < fArr.size(); i++) {
            graphs.add(deserializeGraph(fArr.get(i)));
        }

        Map<IGraph, IBloomFilter<String>> bs = new HashMap<>();
        JsonArray bArr = obj.getAsJsonArray("bs");
        for(int i = 0; i < bArr.size(); i++) {
            bs.put(deserializeGraph(bArr.get(i).getAsJsonObject().get("graph")),
                    PrefixPartitionedBloomFilter.create(bArr.get(i).getAsJsonObject().get("filename").getAsString()));
        }

        Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = new HashMap<>();
        JsonArray blArr = obj.getAsJsonArray("blooms");
        for(int i = 0; i < blArr.size(); i++) {
            blooms.put(new Tuple<>(deserializeGraph(blArr.get(i).getAsJsonObject().get("graph1")), deserializeGraph(blArr.get(i).getAsJsonObject().get("graph2"))),
                    PrefixPartitionedBloomFilter.create(blArr.get(i).getAsJsonObject().get("filename").getAsString()));
        }

        return new PpbfIndex(blooms, bs, graphs);
    }

    private JsonElement serializeGraph(IGraph graph) {
        JsonObject obj = new JsonObject();

        obj.addProperty("community", graph.getCommunity());
        obj.addProperty("baseUri", graph.getBaseUri());
        obj.addProperty("id", graph.getId());

        return obj;
    }

    private IGraph deserializeGraph(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        return new Graph(obj.get("community").getAsString(), obj.get("baseUri").getAsString(), obj.get("id").getAsString());
    }

    private JsonElement serializeTransactions(Map<String, Tuple<ITransaction, Set<String>>> ts) {
        JsonArray arr = new JsonArray();

        for(String s : ts.keySet()) {
            JsonObject obj = new JsonObject();

            obj.addProperty("key", s);
            obj.add("transaction", serializeTransaction(ts.get(s).getFirst()));

            JsonArray aArr = new JsonArray();
            for(String ss : ts.get(s).getSecond()) {
                aArr.add(ss);
            }
            obj.add("nodes", aArr);

            arr.add(obj);
        }

        return arr;
    }

    private Map<String, Tuple<ITransaction, Set<String>>> deserializeTransactions(JsonElement element) {
        Map<String, Tuple<ITransaction, Set<String>>> ret = new HashMap<>();
        JsonArray arr = element.getAsJsonArray();

        for(int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.get(i).getAsJsonObject();
            String key = obj.get("key").getAsString();
            ITransaction t = deserializeTransaction(obj.get("transaction"));

            Set<String> nodes = new HashSet<>();
            JsonArray nArr = obj.getAsJsonArray("nodes");
            for(int j = 0; j < nArr.size(); j++) {
                nodes.add(nArr.get(j).getAsString());
            }

            ret.put(key, new Tuple<>(t, nodes));
        }

        return ret;
    }

    private JsonElement serializeCommunity(Community c) {
        JsonObject obj = new JsonObject();

        obj.addProperty("id", c.getId());
        obj.addProperty("name", c.getName());
        obj.addProperty("memberType", c.getMemberType().toString());

        Set<String> fids = c.getFragmentIds();
        JsonArray fArr = new JsonArray();
        for(String fid : fids) {
            fArr.add(fid);
        }
        obj.add("fragments", fArr);

        Set<CommunityMember> parts = c.getParticipants();
        JsonArray pArr = new JsonArray();
        for(CommunityMember m : parts) {
            pArr.add(serializeCommunityMember(m));
        }
        obj.add("participants", pArr);

        Set<CommunityMember> obs = c.getObservers();
        JsonArray oArr = new JsonArray();
        for(CommunityMember m : obs) {
            oArr.add(serializeCommunityMember(m));
        }
        obj.add("observers", oArr);

        return obj;
    }

    private Community deserializeCommunity(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();

        String id = obj.get("id").getAsString();
        String name = obj.get("name").getAsString();
        Community.MemberType type = Community.MemberType.valueOf(obj.get("memberType").getAsString());

        Set<CommunityMember> parts = new HashSet<>();
        Set<CommunityMember> obs = new HashSet<>();

        JsonArray arr = obj.getAsJsonArray("participants");
        for(int i = 0; i < arr.size(); i++) {
            parts.add(deserializeCommunityMember(arr.get(i)));
        }
        arr = obj.getAsJsonArray("observers");
        for(int i = 0; i < arr.size(); i++) {
            obs.add(deserializeCommunityMember(arr.get(i)));
        }

        Set<String> frags = new HashSet<>();
        arr = obj.getAsJsonArray("fragments");
        for(int i = 0; i < arr.size(); i++) {
            frags.add(arr.get(i).getAsString());
        }


        return new Community(id, name, type, parts, obs, frags);
    }

    private JsonElement serializeCommunityMember(CommunityMember m) {
        JsonObject obj = new JsonObject();

        obj.addProperty("id", m.getId());
        obj.addProperty("address", m.getAddress());

        return obj;
    }

    private CommunityMember deserializeCommunityMember(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        return new CommunityMember(obj.get("id").getAsString(), obj.get("address").getAsString());
    }

    private JsonElement serializeKeys(KeyPair keys) {
        JsonObject obj = new JsonObject();

        byte[] pub = keys.getPublic().getEncoded();
        byte[] priv = keys.getPrivate().getEncoded();

        JsonArray arrPub = new JsonArray(pub.length);
        for(byte b : pub) {
            arrPub.add(b);
        }

        JsonArray arrPriv = new JsonArray(priv.length);
        for(byte b : priv) {
            arrPriv.add(b);
        }

        obj.add("public", arrPub);
        obj.add("private", arrPriv);

        return obj;
    }

    private KeyPair deserializeKeys(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        JsonArray arrPub = obj.getAsJsonArray("public");
        JsonArray arrPriv = obj.getAsJsonArray("private");

        byte[] pub = new byte[arrPub.size()];
        byte[] priv = new byte[arrPriv.size()];

        for(int i = 0; i < arrPub.size(); i++) {
            pub[i] = arrPub.get(i).getAsByte();
        }

        for(int i = 0; i < arrPriv.size(); i++) {
            priv[i] = arrPriv.get(i).getAsByte();
        }

        PublicKey pKey = CryptoUtils.getPublicKey(pub);
        PrivateKey oKey = CryptoUtils.getPrivateKey(priv);

        return new KeyPair(pKey, oKey);
    }

    private JsonElement serializeChains(Map<String, KnowledgeChain> chains) {
        JsonArray arr = new JsonArray();

        for(String key : chains.keySet()) {
            JsonObject obj = new JsonObject();
            obj.addProperty("key", key);

            JsonObject chainObj = new JsonObject();
            KnowledgeChain chain = chains.get(key);

            byte[] cKey = chain.getKey().getEncoded();
            JsonArray arrKey = new JsonArray(cKey.length);
            for(byte b : cKey) {
                arrKey.add(b);
            }
            chainObj.add("key", arrKey);
            chainObj.addProperty("file", ((HdtDataSource)chain.getDatasource()).getFile());
            chainObj.add("entry", serializeChainEntry(chain.top()));

            obj.add("chain", chainObj);
            arr.add(obj);
        }

        return arr;
    }

    private Map<String, KnowledgeChain> deserializeChains(JsonElement element) {
        Map<String, KnowledgeChain> chains = new HashMap<>();
        JsonArray arr = element.getAsJsonArray();

        for(int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.get(i).getAsJsonObject();
            String id = obj.get("key").getAsString();

            JsonObject chainObj = obj.getAsJsonObject("chain");

            ChainEntry entry = deserializeChainEntry(chainObj.get("entry"));
            IDataSource dataSource;
            try {
                dataSource = DataSourceFactory.createLocal(id, chainObj.get("file").getAsString());
            } catch (IOException e) {
                continue;
            }

            JsonArray bArr = chainObj.getAsJsonArray("key");
            byte[] key = new byte[bArr.size()];
            for(int j = 0; j < bArr.size(); j++) {
                key[j] = bArr.get(j).getAsByte();
            }

            KnowledgeChain chain = new KnowledgeChain(entry, dataSource, key);
            chains.put(id, chain);
        }

        return chains;
    }

    private JsonElement serializeChainEntry(ChainEntry entry) {
        JsonObject obj = new JsonObject();

        if(entry.isFirst()) {
            obj.addProperty("transaction", "null");
            obj.addProperty("prev", "null");
            return obj;
        }

        obj.add("transaction", serializeTransaction(entry.getTransaction()));
        obj.add("prev", serializeChainEntry(entry.previous()));

        return obj;
    }

    private ChainEntry deserializeChainEntry(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        if(!obj.get("transaction").isJsonObject() && !obj.get("prev").isJsonObject())
            return ChainEntry.getInitialEntry();

        JsonObject transObj = obj.getAsJsonObject("transaction");
        JsonObject prevObj = obj.getAsJsonObject("prev");

        if((!transObj.isJsonObject() && transObj.getAsString().equals("null")) &&
                (!prevObj.isJsonObject() && prevObj.getAsString().equals("null")))
            return ChainEntry.getInitialEntry();

        return new ChainEntry(deserializeTransaction(transObj), deserializeChainEntry(prevObj));
    }

    private JsonElement serializeTransaction(ITransaction transaction) {
        JsonObject obj = new JsonObject();

        obj.addProperty("fragment", transaction.getFragmentId());
        obj.addProperty("author", transaction.getAuthor());
        obj.addProperty("id", transaction.getId());

        JsonArray arr = new JsonArray();
        List<Operation> ops = transaction.getOperations();
        for(Operation op : ops) {
            arr.add(serializeOperation(op));
        }
        obj.add("operations", arr);

        return obj;
    }

    private ITransaction deserializeTransaction(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        String fid = obj.get("fragment").getAsString();
        String author = obj.get("author").getAsString();
        String id = obj.get("id").getAsString();

        List<Operation> lst = new ArrayList<>();
        JsonArray arr = obj.get("operations").getAsJsonArray();
        for(int i = 0; i < arr.size(); i++) {
            lst.add(deserializeOperation(arr.get(i)));
        }

        return TransactionFactory.getTransaction(lst, fid, author, id);
    }

    private JsonElement serializeOperation(Operation operation) {
        JsonObject obj = new JsonObject();

        obj.addProperty("type", operation.getType().toString());
        obj.addProperty("subject", operation.getTriple().getSubject());
        obj.addProperty("predicate", operation.getTriple().getPredicate());
        obj.addProperty("object", operation.getTriple().getObject());

        return obj;
    }

    private Operation deserializeOperation(JsonElement element) {
        JsonObject obj = element.getAsJsonObject();
        return new Operation(Operation.OperationType.valueOf(obj.get("type").getAsString()),
                new Triple(obj.get("subject").getAsString(), obj.get("predicate").getAsString(), obj.get("object").getAsString()));
    }
}
