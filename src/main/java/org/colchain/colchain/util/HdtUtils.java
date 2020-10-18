package org.colchain.colchain.util;

import org.colchain.colchain.community.Community;
import org.colchain.colchain.community.CommunityMember;
import org.colchain.colchain.node.AbstractNode;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

public class HdtUtils {
    private static RandomString gen = new RandomString();
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    public static void upload(String path, String community) throws IOException {
        System.out.println("Uploading " + path);
        HDT hdt = HDTManager.mapIndexedHDT(path, null);

        DictionarySection predicates = hdt.getDictionary().getPredicates();
        System.out.println("Found " + predicates.getNumberOfElements() + " predicates / fragments");

        int count = predicates.getNumberOfElements();
        int num = 1;
        Iterator<? extends CharSequence> it = predicates.getSortedEntries();
        while(it.hasNext()) {
            CharSequence pred = it.next();
            System.out.println("Predicate " + num + "/" + count);
            num++;

            String id = gen.nextString();
            String outpath = AbstractNode.getState().getDatastore() + "hdt/" + id + ".hdt";

            IteratorTripleString triples;
            HDT newHdt;
            try {
                triples = hdt.search("", pred, "");
                newHdt = HDTManager.generateHDT(triples, "http://colchain.org/fragments#" + id, new HDTSpecification(), null);
            } catch (Exception e) {
                continue;
            }

            newHdt.saveToHDT(outpath, null);
            System.out.println("Saved file: " + outpath);

            newHdt.close();

            if(pred.toString().equals("http://purl.org/spar/pro/withRole"))
                System.out.println("Found the fragment. " + id);
            AbstractNode.getState().addNewFragment(id, pred.toString(), outpath, community, AbstractNode.getState().getPublicKey());

            Community c = AbstractNode.getState().getCommunity(community);
            Set<CommunityMember> parts = c.getParticipants();
            for (CommunityMember m : parts) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=newFragment&id="
                        + id + "&address=" + AbstractNode.getState().getAddress() + "&community=" + community;
                performAction(a);
            }

            Set<CommunityMember> obs = c.getObservers();
            for (CommunityMember m : obs) {
                if(m.getAddress().equals(AbstractNode.getState().getAddress())) continue;
                String a = m.getAddress() + (m.getAddress().endsWith("/") ? "" : "/") + "api/community?mode=newFragment&id="
                        + id + "&address=" + AbstractNode.getState().getAddress() + "&community=" + community;
                performAction(a);
            }
        }
    }

    private static void performAction(String address) {
        HttpGet request = new HttpGet(address);
        try {
            httpClient.execute(request);
        } catch (IOException e) {
        }

    }
}
