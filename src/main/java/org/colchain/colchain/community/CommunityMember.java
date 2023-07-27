package org.colchain.colchain.community;

import com.google.gson.Gson;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.transaction.ITransaction;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.http.client.fluent.Request;

import java.io.IOException;
import java.util.Objects;

public class CommunityMember {
    private final String id;
    private String address;
    private static URLCodec urlCodec = new URLCodec("utf8");

    public CommunityMember(String id) {
        this.id = id;
        this.address = "";
    }

    public CommunityMember(String id, String address) {
        this.id = id;
        this.address = address;
    }

    public String getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public void suggestTransaction(ITransaction t, byte[] signature) {
        Gson gson = new Gson();
        String jsonT, jsonS;
        try {
            jsonT = urlCodec.encode(gson.toJson(t));
            jsonS = urlCodec.encode(gson.toJson(signature));
        } catch (EncoderException e) {
            return;
        }

        String url = address + (address.endsWith("/")? "" : "/") + "api/update?mode=new&trans=" + jsonT + "&sign=" + jsonS;
        try {
            Request.Get(url).execute();
        } catch (IOException e) {}
    }

    public void accept(String tid) {
        String url = address + (address.endsWith("/")? "" : "/") + "api/update?mode=accept&trans=" + tid + "&node=" + AbstractNode.getState().getId();
        try {
            Request.Get(url).execute();
        } catch (IOException e) {}
    }

    public void updateIndex(String fid) {
        try {
            String url = address + (address.endsWith("/")? "" : "/") + "api/update?mode=index&id=" + fid + "&address=" + urlCodec.encode(AbstractNode.getState().getAddress());
            Request.Get(url).execute();
        } catch (IOException | EncoderException e) {}
    }

    public void updateFragment(String fid) {
        try {
            String url = address + (address.endsWith("/")? "" : "/") + "api/update?mode=fragment&id=" + fid + "&address=" + urlCodec.encode(AbstractNode.getState().getAddress());
            Request.Get(url).execute();
        } catch (IOException | EncoderException e) {}
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommunityMember that = (CommunityMember) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, address);
    }
}
