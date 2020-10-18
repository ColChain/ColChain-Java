package org.colchain.colchain.sparql;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class ColchainJenaConstants {
    private static final String BASE_URI = "http://colchain.org/";
    public static final Resource COLCHAIN_GRAPH = ResourceFactory.createResource(BASE_URI+"fuseki#ColChainGraph") ;
    public static long NTB = 0;
    public static int NEM = 0;
    public final static int BIND_NUM = 30;
}
