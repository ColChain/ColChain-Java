package org.colchain.colchain.sparql.graph;

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
import org.rdfhdt.hdt.hdt.HDT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ColchainGraph extends GraphBase {
    private static final Logger log = LoggerFactory.getLogger(ColchainGraph.class);
    private ColchainStatistics statistics = new ColchainStatistics(this);
    private static ColchainCapabilities capabilities = new ColchainCapabilities();
    private ReorderTransformation reorderTransform;
    private static URLCodec urlCodec = new URLCodec("utf8");
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
                if(timeIncluded)
                    datasource = AbstractNode.getState().getDatasource(graph.getId(), timestamp).getHdt();
                else
                    datasource = AbstractNode.getState().getDatasource(graph.getId()).getHdt();
                queue.add(new LocalColchainIterator(jenaTriple, bindings, datasource));
            } else {
                String url = "";
                try {
                    url = getFragmentUrl(jenaTriple, bindings, graph);
                } catch (EncoderException e) {
                    continue;
                }

                queue.add(new RemoteColchainIterator(url, jenaTriple, bindings));
            }
        }

        return new ColchainJenaIterator(queue);
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
