package org.colchain.colchain.sparql.graph;

import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.jena.n3.turtle.parser.ParseException;
import org.apache.jena.n3.turtle.parser.TurtleParser;
import org.colchain.colchain.sparql.ColchainTurtleEventHandler;
import org.colchain.index.graph.IGraph;
import org.colchain.colchain.community.Community;
import org.colchain.colchain.node.AbstractNode;
import org.colchain.colchain.sparql.ColchainBindings;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.colchain.colchain.sparql.iter.ColchainJenaIterator;
import org.colchain.colchain.sparql.iter.EmptyIterator;
import org.colchain.colchain.sparql.iter.LocalColchainIterator;
import org.colchain.colchain.sparql.iter.RemoteColchainIterator;
import org.colchain.colchain.sparql.solver.ColchainEngine;
import org.colchain.colchain.sparql.solver.OpExecutorColchain;
import org.colchain.colchain.sparql.solver.ReorderTransformationColchain;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.ext.com.google.common.collect.Sets;
import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.colchain.index.util.Tuple;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class ColchainGraph extends GraphBase {
    private static final Logger log = LoggerFactory.getLogger(ColchainGraph.class);
    private ColchainStatistics statistics = new ColchainStatistics(this);
    private static ColchainCapabilities capabilities = new ColchainCapabilities();
    private ReorderTransformation reorderTransform;
    private static URLCodec urlCodec = new URLCodec("utf8");
    private final static Map<Tuple<String, Long>, HDT> FILE_CACHE = new HashMap<>();
    private final static int THRESHOLD = 100;
    private long timestamp = 0;
    private boolean timeIncluded = false;

    static {
        QC.setFactory(ARQ.getContext(), OpExecutorColchain.opExecFactoryColchain);
        ColchainEngine.register();
    }

    public ColchainGraph() {
        ColchainJenaConstants.NEM = 0;
        ColchainJenaConstants.NTB = 0;
        reorderTransform = new ReorderTransformationColchain(this);
    }

    public ColchainGraph(long timestamp) {
        ColchainJenaConstants.NEM = 0;
        ColchainJenaConstants.NTB = 0;
        reorderTransform = new ReorderTransformationColchain(this);
        this.timestamp = timestamp;
        this.timeIncluded = true;
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple jenaTriple) {
        return new EmptyIterator();
    }

    public ReorderTransformation getReorderTransform() {
        return reorderTransform;
    }

    public ExtendedIterator<Pair<Triple, Binding>> graphBaseFind(Triple jenaTriple, ColchainBindings bindings, Set<IGraph> fragments) {
        Queue<NiceIterator<Pair<Triple, Binding>>> queue = new LinkedList<>();
        for (IGraph graph : fragments) {
            String cid = graph.getCommunity();
            Community community = AbstractNode.getState().getCommunity(cid);
            if (community.getMemberType() == Community.MemberType.PARTICIPANT) {
                HDT datasource;
                if (timeIncluded)
                    datasource = AbstractNode.getState().getDatasource(graph.getId(), timestamp).getHdt();
                else
                    datasource = AbstractNode.getState().getDatasource(graph.getId()).getHdt();
                queue.add(new LocalColchainIterator(jenaTriple, bindings, datasource));
            } /*else if ((timeIncluded && FILE_CACHE.containsKey(new Tuple<>(graph.getId() + ".hdt", timestamp)))) {
                queue.add(new LocalColchainIterator(jenaTriple, bindings, FILE_CACHE.get(new Tuple<>(graph.getId() + ".hdt", timestamp))));
            } else if (FILE_CACHE.containsKey(new Tuple<>(graph.getId() + ".hdt", -1L))) {
                queue.add(new LocalColchainIterator(jenaTriple, bindings, FILE_CACHE.get(new Tuple<>(graph.getId() + ".hdt", -1L))));
            }*/ else {
                String url = "";
                try {
                    url = getFragmentUrl(jenaTriple, bindings, graph);
                } catch (EncoderException e) {
                    continue;
                }

                /*int numResults = getNumResults(url);
                if (numResults > THRESHOLD) {
                    String filename = graph.getId() + ".hdt";
                    downloadFragment(graph);

                    if (timeIncluded)
                        queue.add(new LocalColchainIterator(jenaTriple, bindings, FILE_CACHE.get(new Tuple<>(filename, timestamp))));
                    else
                        queue.add(new LocalColchainIterator(jenaTriple, bindings, FILE_CACHE.get(new Tuple<>(filename, -1L))));
                } else {*/
                    queue.add(new RemoteColchainIterator(url, jenaTriple, bindings));
                //}
            }
        }

        return new ColchainJenaIterator(queue);
    }

    private void downloadFragment(IGraph graph) {
        String url = getFragmentUrl(graph, false);
        String filePathString = AbstractNode.getState().getDatastore() + (AbstractNode.getState().getDatastore().endsWith("/") ? "" : "/") + "cache/";
        new File(filePathString).mkdirs();
        filePathString = filePathString + graph.getId() + (timeIncluded ? "-" + timestamp : "") + ".hdt";
        downloadFile(url, filePathString);

        // Download index
        url = getFragmentUrl(graph, true);
        filePathString = filePathString + ".index.v1-1";
        downloadFile(url, filePathString);

        try {
            if (timeIncluded)
                FILE_CACHE.put(new Tuple<>(graph.getId() + ".hdt", timestamp), HDTManager.mapIndexedHDT(filePathString.replace(".index.v1-1", "")));
            else
                FILE_CACHE.put(new Tuple<>(graph.getId() + ".hdt", -1L), HDTManager.mapIndexedHDT(filePathString.replace(".index.v1-1", "")));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void downloadFile(String url, String filePathString) {
        Content content = null;
        try {
            ColchainJenaConstants.NEM++;
            content = Request.Get(url).addHeader("accept", "text/hdt").execute().returnContent();
            ColchainJenaConstants.NTB += content.asBytes().length;
        } catch (IOException e) {
            return;
        }

        InputStream stream = content.asStream();
        try {
            Files.copy(stream, Paths.get(".", filePathString), StandardCopyOption.REPLACE_EXISTING);
            stream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getFragmentUrl(IGraph graph, boolean index) {
        StringBuilder sb = new StringBuilder();
        String address = AbstractNode.getState().getCommunity(graph.getCommunity()).getParticipant().getAddress();

        sb.append(address + (address.endsWith("/") ? "" : "/"));
        sb.append("fragment/" + graph.getId() + ".hdt");

        if (index) {
            sb.append(".index.v1-1");
        }

        if (timeIncluded) {
            sb.append("?");
            sb.append("time=" + timestamp);
        }

        return sb.toString();
    }

    private int getNumResults(String url) {
        Content content = null;
        try {
            ColchainJenaConstants.NEM++;
            content = Request.Get(url).addHeader("accept", "text/turtle").execute().returnContent();
            ColchainJenaConstants.NTB += content.asBytes().length;
        } catch (IOException e) {
            return 0;
        }

        TurtleParser parser = new TurtleParser(content.asStream());
        ColchainTurtleEventHandler handler = new ColchainTurtleEventHandler(url);
        parser.setEventHandler(handler);
        try {
            parser.parse();
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }

        return handler.getNumResults();
    }

    private String getFragmentUrl(Triple jenaTriple, ColchainBindings bindings, IGraph graph) throws EncoderException {
        StringBuilder sb = new StringBuilder();
        boolean isQuestionMarkAdded = false;
        String address = AbstractNode.getState().getCommunity(graph.getCommunity()).getParticipant().getAddress();
        sb.append(address + (address.endsWith("/") ? "" : "/"));
        sb.append("ldf/" + graph.getId());

        isQuestionMarkAdded = appendUrlParam(sb, jenaTriple.getSubject(), "subject",
                isQuestionMarkAdded);
        isQuestionMarkAdded = appendUrlParam(sb, jenaTriple.getPredicate(),
                "predicate", isQuestionMarkAdded);
        isQuestionMarkAdded =
                appendUrlParam(sb, jenaTriple.getObject(), "object", isQuestionMarkAdded);
        if (bindings.size() > 0) {
            appendBindings(jenaTriple, sb, bindings);
        }

        if (timeIncluded) {
            isQuestionMarkAdded = appendStringParam(sb, "" + timestamp, "time", isQuestionMarkAdded);
        }

        return sb.toString();
    }

    private void appendBindings(Triple triple, StringBuilder sb, ColchainBindings bindings) throws EncoderException {
        if (bindings.size() > 0 && !bindings.get(0).isEmpty()) {
            Set<String> varsInTP = new HashSet<>();
            Binding first = bindings.get(0);
            if (triple.getSubject().isVariable())
                varsInTP.add("?" + triple.getSubject().getName());
            if (triple.getPredicate().isVariable())
                varsInTP.add("?" + triple.getPredicate().getName());
            if (triple.getObject().isVariable())
                varsInTP.add("?" + triple.getObject().getName());

            StringBuilder valuesSb = new StringBuilder();

            Set<String> boundVars = new HashSet<>();
            Iterator<Var> it = bindings.get(0).vars();
            while (it.hasNext()) {
                Var v = it.next();
                boundVars.add("?" + v.getVarName());
            }
            ArrayList<String> varsInURL = new ArrayList<String>(Sets.intersection(varsInTP, boundVars));
            if (varsInURL.size() == 0) return;

            valuesSb.append("(");
            String varStr = String.join(" ", varsInURL);
            if (triple.getSubject().isVariable())
                varStr = varStr.replace(triple.getSubject().toString(), "?subject");
            if (triple.getPredicate().isVariable())
                varStr = varStr.replace(triple.getPredicate().toString(), "?predicate");
            if (triple.getObject().isVariable())
                varStr = varStr.replace(triple.getObject().toString(), "?object");

            valuesSb.append(varStr);
            valuesSb.append("){");

            Set<ArrayList<String>> set = new HashSet<>();
            for (int i = 0; i < bindings.size(); i++) {
                ArrayList<String> bindingsStrList = new ArrayList<String>();
                Binding binding = bindings.get(i);
                for (int j = 0; j < varsInURL.size(); j++) {
                    Iterator<Var> ii = binding.vars();
                    while (ii.hasNext()) {
                        Var v = ii.next();
                        String varname = "?" + v.getVarName();
                        if (varname.equals(varsInURL.get(j)))
                            bindingsStrList.add("<" + binding.get(v).toString() + ">");
                    }
                }
                if (set.contains(bindingsStrList)) continue;
                set.add(bindingsStrList);
                valuesSb.append("(");
                valuesSb.append(String.join(" ", bindingsStrList));
                valuesSb.append(")");
            }
            valuesSb.append("}");
            sb.append("&").append("values").append("=").append(urlCodec.encode(valuesSb.toString()));
        }

    }

    private boolean appendUrlParam(StringBuilder sb, Node node, String paramName,
                                   Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            if (node.isVariable())
                sb.append("&").append(paramName).append("=?").append(node.getName());
            else if (node.isLiteral())
                sb.append("&").append(paramName).append("=").append(urlCodec.encode("\"" + node.getLiteral().toString() + "\""));
            else
                sb.append("&").append(paramName).append("=").append(urlCodec.encode(node.getURI()));
        } else {
            if (node.isVariable()) {
                sb.append("?").append(paramName).append("=?").append(node.getName());
                return true;
            } else if (node.isLiteral()) {
                sb.append("?").append(paramName).append("=").append("\"" + urlCodec.encode(node.getLiteral().toString() + "\""));
                return true;
            } else {
                sb.append("?").append(paramName).append("=").append(urlCodec.encode(node.getURI()));
                return true;
            }
        }
        return isQuestionMarkAdded;
    }

    private boolean appendStringParam(StringBuilder sb, String str, String paramName,
                                      Boolean isQuestionMarkAdded) throws EncoderException {
        if (isQuestionMarkAdded) {
            sb.append("&").append(paramName).append("=").append(urlCodec.encode(str));
        } else {
            sb.append("?").append(paramName).append("=").append(urlCodec.encode(str));
            return true;
        }
        return isQuestionMarkAdded;
    }

    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        return statistics;
    }

    @Override
    public Capabilities getCapabilities() {
        return capabilities;
    }

    @Override
    protected int graphBaseSize() {
        //return (int)statistics.getStatistic(Node.ANY, Node.ANY, Node.ANY);
        return 1000000000;
    }

    @Override
    public void close() {
        super.close();
    }
}
