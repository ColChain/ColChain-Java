package org.colchain.colchain.node;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.node.impl.NodeImpl;
import org.colchain.colchain.util.NodeSerializer;
import org.linkeddatafragments.datasource.IDataSource;

import java.io.*;
import java.util.HashMap;
import java.util.Set;

public abstract class AbstractNode implements INode {
    static INode state = new NodeImpl();
    public static INode getState() {
        return state;
    }

    private String datastore = "";
    private String address = "";

    @Override
    public String getDatastore() {
        return datastore;
    }

    @Override
    public void setDatastore(String datastore) {
        this.datastore = datastore;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getAddressPath() {
        if(address.endsWith("/")) return address;
        return address + "/";
    }

    @Override
    public void setAddress(String address) {
        this.address = address;
    }

    public abstract IDataSource getDatasource(String id);
    public abstract HashMap<String, IDataSource> getDatasources();
    public abstract Set<String> getDatasourceIds();
    public abstract void addNewFragment(String id, String predicate, String path, String community, byte[] key);

    public static void loadState(String filename) {
        StringBuilder sb = new StringBuilder();

        try {
            InputStream is = new FileInputStream(filename);
            BufferedReader in = new BufferedReader(new InputStreamReader(is));

            String line = in.readLine();
            while(line != null) {
                sb.append(line);
                line = in.readLine();
            }
        } catch (IOException e) {
            return;
        }

        String json = sb.toString();

        Gson gson = new GsonBuilder().registerTypeAdapter(INode.class, new NodeSerializer()).create();
        state = gson.fromJson(json, INode.class);
    }

    @Override
    public CommunityMember getAsCommunityMember() {
        return new CommunityMember(this.getId(), address);
    }
}
