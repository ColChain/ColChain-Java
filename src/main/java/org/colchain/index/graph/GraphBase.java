package org.colchain.index.graph;

public abstract class GraphBase implements IGraph {
    private String community;
    private String baseUri;
    private String id;

    public GraphBase(String community, String baseUri, String id) {
        this.community = community;
        this.baseUri = baseUri;
        this.id = id;
    }



    @Override
    public String getCommunity() {
        return community;
    }

    @Override
    public String getBaseUri() {
        return baseUri;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isCommunity(String id) {
        return community.equals(id);
    }

    @Override
    public int hashCode() {
        return baseUri.hashCode() + id.hashCode();
    }



    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != GraphBase.class) return false;
        GraphBase other = (GraphBase) obj;
        return baseUri.equals(other.baseUri) && id.equals(other.id);
    }

    @Override
    public String toString() {
        return "GraphBase{" +
                "community=" + community +
                ", baseUri='" + baseUri + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
