package org.linkeddatafragments.fragments.tpf;

import org.apache.jena.rdf.model.RDFNode;
import org.linkeddatafragments.util.TriplePatternElementParserForJena;

 
public class TPFRequestParserForJenaBackends
    extends TPFRequestParser<RDFNode,String,String>
{
    private static TPFRequestParserForJenaBackends instance = null;

    /**
     *
     * @return
     */
    public static TPFRequestParserForJenaBackends getInstance()
    {
        if ( instance == null ) {
            instance = new TPFRequestParserForJenaBackends();
        }
        return instance;
    }

    /**
     *
     */
    protected TPFRequestParserForJenaBackends()
    {
        super( TriplePatternElementParserForJena.getInstance() );
    }
}
