package org.linkeddatafragments.fragments.tpf;

import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.util.TriplePatternElementParserForJena;

public class BRTPFRequestParserForJenaBackends
        extends BRTPFRequestParser<RDFNode, String, String> {
    private static BRTPFRequestParserForJenaBackends instance = null;

    /**
     *
     * @return
     */
    public static BRTPFRequestParserForJenaBackends getInstance()
    {
        if ( instance == null ) {
            instance = new BRTPFRequestParserForJenaBackends();
        }
        return instance;
    }

    /**
     *
     */
    protected BRTPFRequestParserForJenaBackends()
    {
        super( TriplePatternElementParserForJena.getInstance() );
    }
}
