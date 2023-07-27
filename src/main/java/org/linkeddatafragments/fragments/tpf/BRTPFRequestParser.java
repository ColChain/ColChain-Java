package org.linkeddatafragments.fragments.tpf;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.util.TriplePatternElementParser;

import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;

public class BRTPFRequestParser<ConstantTermType,NamedVarType,AnonVarType>
        extends FragmentRequestParserBase {
    public final TriplePatternElementParser<ConstantTermType,NamedVarType,AnonVarType> elmtParser;

    /**
     *
     * @param elmtParser
     */
    public BRTPFRequestParser(
            final TriplePatternElementParser<ConstantTermType,NamedVarType,AnonVarType> elmtParser )
    {
        this.elmtParser = elmtParser;
    }

    /**
     *
     * @param httpRequest
     * @param config
     * @return
     * @throws IllegalArgumentException
     */
    @Override
    protected Worker getWorker(final HttpServletRequest httpRequest,
                                                final ConfigReader config )
            throws IllegalArgumentException
    {
        return new Worker( httpRequest, config );
    }

    /**
     *
     */
    protected class Worker extends FragmentRequestParserBase.Worker
    {

        /**
         *
         * @param request
         * @param config
         */
        public Worker( final HttpServletRequest request,
                       final ConfigReader config )
        {
            super( request, config );
        }

        /**
         *
         * @return
         * @throws IllegalArgumentException
         */
        @Override
        public ILinkedDataFragmentRequest createFragmentRequest()
                throws IllegalArgumentException
        {
            // System.out.println("Create Fragment Request :)");
            return new BindingsRestrictedTriplePatternFragmentRequestImpl<ConstantTermType,NamedVarType,AnonVarType>(
                    getFragmentURL(),
                    getDatasetURL(),
                    pageNumberWasRequested,
                    pageNumber,
                    getSubject(),
                    getPredicate(),
                    getObject(),
                    getBindings(),
                    getVars());
        }

        /**
         *
         * @return
         */
        public ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType> getSubject() {
            return getParameterAsTriplePatternElement(
                    ITriplePatternFragmentRequest.PARAMETERNAME_SUBJ );
        }

        /**
         *
         * @return
         */
        public ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType> getPredicate() {
            return getParameterAsTriplePatternElement(
                    ITriplePatternFragmentRequest.PARAMETERNAME_PRED );
        }

        /**
         *
         * @return
         */
        public ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType> getObject() {
            return getParameterAsTriplePatternElement(
                    ITriplePatternFragmentRequest.PARAMETERNAME_OBJ );
        }

        public List<Binding> getBindings() {
            final List<Var> foundVariables = new ArrayList<Var>();
            return parseAsSetOfBindings(
                    request.getParameter("values"),
                    foundVariables);
        }

        public List<Var> getVars() {
            return parseAsSetOfVars(request.getParameter("values"));
        }

        /**
         *
         * @param paramName
         * @return
         */
        public ITriplePatternElement<ConstantTermType,NamedVarType,AnonVarType>
        getParameterAsTriplePatternElement( final String paramName )
        {
            final String parameter = request.getParameter( paramName );
            return elmtParser.parseIntoTriplePatternElement( parameter );
        }

        /**
         * Parses the given value as set of bindings.
         *
         * @param value          containing the SPARQL bindings
         * @param foundVariables a list with variables found in the VALUES clause
         * @return a list with solution mappings found in the VALUES clause
         */
        private List<Binding> parseAsSetOfBindings(final String value, final List<Var> foundVariables) {
            if (value == null) {
                return null;
            }
            String newString = "select * where {} VALUES " + value;
            Query q = QueryFactory.create(newString);
            foundVariables.addAll(q.getValuesVariables());
            return q.getValuesData();
        }

        /**
         * Parses the given value as set of vars.
         *
         * @param value          containing the SPARQL bindings
         * @return a list with solution mappings found in the VALUES clause
         */
        private List<Var> parseAsSetOfVars(final String value) {
            List<Var> foundVariables = new ArrayList<>();
            if (value == null) {
                return null;
            }
            String newString = "select * where {} VALUES " + value;
            Query q = QueryFactory.create(newString);
            foundVariables.addAll(q.getValuesVariables());
            return foundVariables;
        }

    } // end of class Worker
}
