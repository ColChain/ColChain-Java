package org.linkeddatafragments.datasource.hdt;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.*;
import org.apache.jena.shared.InvalidPropertyURIException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.datasource.AbstractRequestProcessorForBindingsRestrictedTriplePatterns;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.tpf.ITriplePatternElement;
import org.linkeddatafragments.fragments.tpf.ITriplePatternFragmentRequest;
import org.linkeddatafragments.fragments.tpf.TriplePatternFragmentImpl;
import org.linkeddatafragments.util.TripleElement;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;
import org.rdfhdt.hdtjena.NodeDictionary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;

import static org.linkeddatafragments.util.CommonResources.INVALID_URI;
import static org.linkeddatafragments.util.RDFTermParser.STRINGPATTERN;

public class HdtBasedRequestProcessorForBRTPFs
        extends AbstractRequestProcessorForBindingsRestrictedTriplePatterns<RDFNode,String,String> {
    /**
     * HDT Datasource
     */
    protected final HDT datasource;

    /**
     * The dictionary
     */
    protected final NodeDictionary dictionary;

    /**
     * Creates the request processor.
     *
     * @throws IOException if the file cannot be loaded
     */
    public HdtBasedRequestProcessorForBRTPFs( HDT hdt, NodeDictionary dict )
    {
        datasource = hdt;
        dictionary = dict;
    }

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getBRTPFSpecificWorker(
            final ITriplePatternFragmentRequest<RDFNode,String,String> request )
            throws IllegalArgumentException
    {
        return new Worker( request );
    }

    /**
     * Worker class for HDT
     */
    protected class Worker
            extends AbstractRequestProcessorForBindingsRestrictedTriplePatterns.Worker<RDFNode,String,String>
    {

        /**
         * Create HDT Worker
         *
         * @param req
         */
        public Worker(
                final ITriplePatternFragmentRequest<RDFNode,String,String> req )
        {
            super( req );
        }

        /**
         * Creates an {@link ILinkedDataFragment} from the HDT
         *
         * @param subject
         * @param predicate
         * @param object
         * @param bindings
         * @param offset
         * @param limit
         * @return
         */
        @Override
        protected ILinkedDataFragment createFragment(
                final ITriplePatternElement<RDFNode,String,String> subject,
                final ITriplePatternElement<RDFNode,String,String> predicate,
                final ITriplePatternElement<RDFNode,String,String> object,
                final List<Binding> bindings,
                final List<Var> vars,
                final long offset,
                final long limit )
        {
            return createFragmentByTriplePatternSubstitution(subject, predicate, object, bindings, vars, offset, limit);
        }

        private ILinkedDataFragment createFragmentByTriplePatternSubstitution(
                final ITriplePatternElement<RDFNode,String,String> subject,
                final ITriplePatternElement<RDFNode,String,String> predicate,
                final ITriplePatternElement<RDFNode,String,String> object,
                final List<Binding> bindings,
                final List<Var> foundVariables,
                final long offset,
                final long limit ) {
            TripleElement _subject = subject.isVariable() ? parseAsResource("?subject") : parseAsResource(subject.asConstantTerm().toString());
            TripleElement _predicate = predicate.isVariable() ? parseAsProperty(predicate.asNamedVariable()) : parseAsProperty(predicate.asConstantTerm().toString());
            TripleElement _object = object.isVariable() ? parseAsNode("?object") : parseAsNode(object.asConstantTerm().toString());

            final TripleIDCachingIterator it = new TripleIDCachingIterator(
                    bindings, foundVariables,
                    _subject, _predicate, _object);

            final Model triples = ModelFactory.createDefaultModel();
            int triplesCheckedSoFar = 0;
            int triplesAddedInCurrentPage = 0;
            boolean atOffset;
            int countBindingsSoFar = 0;

            while (it.hasNext()) {
                final TripleID t = it.next();
                final IteratorTripleID matches = datasource.getTriples().search(t);
                final boolean hasMatches = matches.hasNext();
                if (hasMatches) {
                    matches.goToStart();
                    while (!(atOffset = (triplesCheckedSoFar == offset))
                            && matches.hasNext()) {
                        matches.next();
                        triplesCheckedSoFar++;
                    }
                    // try to add `limit` triples to the result model
                    if (atOffset) {
                        while (triplesAddedInCurrentPage < limit
                                && matches.hasNext()) {
                            triples.add(
                                    triples.asStatement(toTriple(matches.next())));
                            triplesAddedInCurrentPage++;
                        }
                    }
                }
                countBindingsSoFar++;
            }

            final int bindingsSize = bindings.size();
            final long minimumTotal = offset + triplesAddedInCurrentPage + 1;
            final long estimatedTotal;
            if (triplesAddedInCurrentPage < limit) {
                estimatedTotal = offset + triplesAddedInCurrentPage;
            }
//         else // This else block is for testing purposes only. The next else block is the correct one.
//         {
//             estimatedTotal = minimumTotal;
//         }
            else {
                final int THRESHOLD = 10;
                final int maxBindingsToUseInEstimation;
                if (bindingsSize <= THRESHOLD) {
                    maxBindingsToUseInEstimation = bindingsSize;
                } else {
                    maxBindingsToUseInEstimation = THRESHOLD;
                }

                long estimationSum = 0L;
                it.reset();
                int i = 0;
                while (it.hasNext() && i < maxBindingsToUseInEstimation) {
                    i++;
                    estimationSum += estimateResultSetSize(it.next());
                }

                if (bindingsSize <= THRESHOLD) {
                    if (estimationSum <= minimumTotal)
                        estimatedTotal = minimumTotal;
                    else
                        estimatedTotal = estimationSum;
                } else // bindingsSize > THRESHOLD
                {
                    final double fraction = bindingsSize / maxBindingsToUseInEstimation;
                    final double estimationAsDouble = fraction * estimationSum;
                    final long estimation = Math.round(estimationAsDouble);
                    if (estimation <= minimumTotal)
                        estimatedTotal = minimumTotal;
                    else
                        estimatedTotal = estimation;
                }
            }

            final long estimatedValid = estimatedTotal;

            boolean isLastPage = triplesAddedInCurrentPage < limit;
            return new TriplePatternFragmentImpl(triples, estimatedValid, request.getFragmentURL(),
                    request.getDatasetURL(), request.getPageNumber(), isLastPage);
        }

        private long estimateResultSetSize(final TripleID t) {
            final IteratorTripleID matches = datasource.getTriples().search(t);

            if (matches.hasNext())
                return Math.max(matches.estimatedNumResults(), 1L);
            else
                return 0L;
        }

        /**
         * Parses the given value as an RDF resource.
         *
         * @param value the value
         * @return the parsed value, or null if unspecified
         */
        private TripleElement parseAsResource(String value) {
            final TripleElement subject = parseAsNode(value);
            if (subject.object == null) {
                return new TripleElement("null", null);
            }
            if (subject.name.equals("Var")) {
                return subject;
            }
            return subject.object == null || subject.object instanceof Resource ? new TripleElement(
                    "RDFNode", (Resource) subject.object) : new TripleElement(
                    "Property", INVALID_URI);
        }

        /**
         * Parses the given value as an RDF property.
         *
         * @param value the value
         * @return the parsed value, or null if unspecified
         */
        private TripleElement parseAsProperty(String value) {
            // final RDFNode predicateNode = parseAsNode(value);
            final TripleElement predicateNode = parseAsNode(value);
            if (predicateNode.object == null) {
                return new TripleElement("null", null);
            }
            if (predicateNode.name.equals("Var")) {
                return predicateNode;
            }
            if (predicateNode.object instanceof Resource) {
                try {
                    return new TripleElement(
                            "Property",
                            ResourceFactory
                                    .createProperty(((Resource) predicateNode.object)
                                            .getURI()));
                } catch (InvalidPropertyURIException ex) {
                    return new TripleElement("Property", INVALID_URI);
                }
            }
            return predicateNode.object == null ? null : new TripleElement(
                    "Property", INVALID_URI);
        }

        /**
         * Parses the given value as an RDF node.
         *
         * @param value the value
         * @return the parsed value, or null if unspecified
         */
        private TripleElement parseAsNode(String value) {
            // nothing or empty indicates an unknown
            if (value == null || value.length() == 0) {
                return new TripleElement("null", null);
            }
            // find the kind of entity based on the first character
            final char firstChar = value.charAt(0);
            switch (firstChar) {
                // variable or blank node indicates an unknown
                case '?':
                    return new TripleElement(Var.alloc(value.replaceAll("\\?", "")));
                case '_':
                    return null;
                // angular brackets indicate a URI
                case '<':
                    return new TripleElement(
                            ResourceFactory.createResource(value.substring(1,
                                    value.length() - 1)));
                // quotes indicate a string
                case '"':
                    final Matcher matcher = STRINGPATTERN.matcher(value);
                    if (matcher.matches()) {
                        final String body = matcher.group(1);
                        final String lang = matcher.group(2);
                        final String type = matcher.group(3);
                        if (lang != null) {
                            return new TripleElement(
                                    ResourceFactory.createLangLiteral(body, lang));
                        }
                        if (type != null) {
                            return new TripleElement(
                                    ResourceFactory.createTypedLiteral(body,
                                            TypeMapper.getInstance().getSafeTypeByName(type)));
                        }
                        return new TripleElement(
                                ResourceFactory.createPlainLiteral(body));
                    }
                    return new TripleElement("Property", INVALID_URI);
                // assume it's a URI without angular brackets
                default:
                    return new TripleElement(
                            ResourceFactory.createResource(value));
            }
        }

        protected class TripleIDProducingIterator implements Iterator<TripleID> {
            protected final boolean sIsVar, pIsVar, oIsVar;
            protected final boolean canHaveMatches;

            protected final List<Node> sBindings = new ArrayList<Node>();
            protected final List<List<Node>> pBindings = new ArrayList<List<Node>>();
            protected final List<List<List<Node>>> oBindings = new ArrayList<List<List<Node>>>();

            protected int curSubjID, curPredID, curObjID;
            protected int curSubjIdx, curPredIdx, curObjIdx;
            protected List<Node> curPredBindings;
            protected List<Node> curObjBindings;
            protected boolean ready;

            public TripleIDProducingIterator(final List<Binding> jenaSolMaps,
                                             final List<Var> foundVariables,
                                             final TripleElement subjectOfTP,
                                             final TripleElement predicateOfTP,
                                             final TripleElement objectOfTP) {
                int numOfVarsCoveredBySolMaps = 0;

                if (subjectOfTP.var != null) {
                    this.sIsVar = true;
                    if (foundVariables.contains(subjectOfTP.var))
                        numOfVarsCoveredBySolMaps++;
                } else
                    this.sIsVar = false;

                if (predicateOfTP.var != null) {
                    this.pIsVar = true;
                    if (foundVariables.contains(predicateOfTP.var))
                        numOfVarsCoveredBySolMaps++;
                } else
                    this.pIsVar = false;

                if (objectOfTP.var != null) {
                    this.oIsVar = true;
                    if (foundVariables.contains(objectOfTP.var))
                        numOfVarsCoveredBySolMaps++;
                } else
                    this.oIsVar = false;

                final boolean needToCheckForDuplicates = (numOfVarsCoveredBySolMaps < foundVariables.size());

                curSubjID = (subjectOfTP.node == null) ?
                        0 :
                        dictionary.getIntID(subjectOfTP.node,
                                TripleComponentRole.SUBJECT);

                curPredID = (predicateOfTP.node == null) ?
                        0 :
                        dictionary.getIntID(predicateOfTP.node,
                                TripleComponentRole.PREDICATE);

                curObjID = (objectOfTP.node == null) ?
                        0 :
                        dictionary.getIntID(objectOfTP.node,
                                TripleComponentRole.OBJECT);

                canHaveMatches = (curSubjID >= 0) && (curPredID >= 0) && (curObjID >= 0);

                if (canHaveMatches) {
                    for (Binding solMap : jenaSolMaps) {
                        final Node s = sIsVar ? solMap.get(subjectOfTP.var) : null;
                        final Node p = pIsVar ? solMap.get(predicateOfTP.var) : null;
                        final Node o = oIsVar ? solMap.get(objectOfTP.var) : null;

                        final List<Node> pBindingsForS;
                        final List<List<Node>> oBindingsForS;
                        int sIdx;
                        if (!needToCheckForDuplicates
                                || (sIdx = sBindings.indexOf(s)) == -1) {
                            sBindings.add(s);

                            pBindingsForS = new ArrayList<Node>();
                            pBindings.add(pBindingsForS);

                            oBindingsForS = new ArrayList<List<Node>>();
                            oBindings.add(oBindingsForS);
                        } else {
                            pBindingsForS = pBindings.get(sIdx);
                            oBindingsForS = oBindings.get(sIdx);
                        }

                        final List<Node> oBindingsForSP;
                        int pIdx;
                        if (!needToCheckForDuplicates
                                || (pIdx = pBindingsForS.indexOf(p)) == -1) {
                            pBindingsForS.add(p);

                            oBindingsForSP = new ArrayList<Node>();
                            oBindingsForS.add(oBindingsForSP);
                        } else {
                            oBindingsForSP = oBindingsForS.get(pIdx);
                        }

                        int oIdx;
                        if (!needToCheckForDuplicates
                                || (pIdx = oBindingsForSP.indexOf(o)) == -1) {
                            oBindingsForSP.add(o);
                        }
                    }
                }

                reset();
            }

            @Override
            public boolean hasNext() {
                if (ready)
                    return true;

                if (!canHaveMatches)
                    return false;

                do {
                    int prevSubjIdx = curSubjIdx;
                    int prevPredIdx = curPredIdx;

                    curObjIdx++;

                    while (curPredID == -1 || curObjIdx >= curObjBindings.size()) {
                        curPredIdx++;
                        while (curSubjID == -1 || curPredIdx >= curPredBindings.size()) {
                            curSubjIdx++;
                            if (curSubjIdx >= sBindings.size()) {
                                return false;
                            }

                            curPredBindings = pBindings.get(curSubjIdx);
                            curPredIdx = 0;

                            if (sIsVar)
                                curSubjID = dictionary.getIntID(sBindings.get(curSubjIdx),
                                        TripleComponentRole.SUBJECT);
                        }

                        curObjBindings = oBindings.get(curSubjIdx).get(curPredIdx);
                        curObjIdx = 0;

                        if (pIsVar)
                            curPredID = dictionary.getIntID(curPredBindings.get(curPredIdx),
                                    TripleComponentRole.PREDICATE);
                    }

                    if (oIsVar)
                        curObjID = dictionary.getIntID(curObjBindings.get(curObjIdx),
                                TripleComponentRole.OBJECT);
                }
                while (curSubjID == -1 || curPredID == -1 || curObjID == -1);

                ready = true;
                return true;
            }

            @Override
            public TripleID next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                ready = false;
                return new TripleID(curSubjID, curPredID, curObjID);
            }

            public void reset() {
                ready = canHaveMatches;

                if (canHaveMatches) {
                    curSubjIdx = curPredIdx = curObjIdx = 0;

                    curPredBindings = pBindings.get(curSubjIdx);
                    curObjBindings = oBindings.get(curSubjIdx).get(curPredIdx);

                    if (sIsVar)
                        curSubjID = dictionary.getIntID(sBindings.get(curSubjIdx),
                                TripleComponentRole.SUBJECT);

                    if (pIsVar)
                        curPredID = dictionary.getIntID(curPredBindings.get(curPredIdx),
                                TripleComponentRole.PREDICATE);

                    if (oIsVar)
                        curObjID = dictionary.getIntID(curObjBindings.get(curObjIdx),
                                TripleComponentRole.OBJECT);
                }
            }

        } // end of TripleIDProducingIterator

        protected class TripleIDCachingIterator implements Iterator<TripleID> {
            protected final TripleIDProducingIterator it;
            protected final List<TripleID> cache = new ArrayList<TripleID>();

            protected boolean replayMode = false;
            protected int replayIdx = 0;

            public TripleIDCachingIterator(final List<Binding> jenaSolMaps,
                                           final List<Var> foundVariables,
                                           final TripleElement subjectOfTP,
                                           final TripleElement predicateOfTP,
                                           final TripleElement objectOfTP) {
                it = new TripleIDProducingIterator(jenaSolMaps,
                        foundVariables,
                        subjectOfTP,
                        predicateOfTP,
                        objectOfTP);
            }

            @Override
            public boolean hasNext() {
                if (replayMode && replayIdx < cache.size())
                    return true;

                replayMode = false;
                return it.hasNext();
            }

            @Override
            public TripleID next() {
                if (!hasNext())
                    throw new NoSuchElementException();

                if (replayMode) {
                    return cache.get(replayIdx++);
                }

                final TripleID t = it.next();
                cache.add(t);
                return t;
            }

            public void reset() {
                replayMode = true;
                replayIdx = 0;
            }

        } // end of TripleIDCachingIterator
    } // end of Worker

    /**
     * Converts the HDT triple to a Jena Triple.
     *
     * @param tripleId the HDT triple
     * @return the Jena triple
     */
    private Triple toTriple(TripleID tripleId) {
        return new Triple(
                dictionary.getNode(tripleId.getSubject(), TripleComponentRole.SUBJECT),
                dictionary.getNode(tripleId.getPredicate(), TripleComponentRole.PREDICATE),
                dictionary.getNode(tripleId.getObject(), TripleComponentRole.OBJECT)
        );
    }
}
