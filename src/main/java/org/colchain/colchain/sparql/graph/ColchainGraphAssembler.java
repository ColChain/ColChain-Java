package org.colchain.colchain.sparql.graph;

import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColchainGraphAssembler extends AssemblerBase implements Assembler {
    private static final Logger log = LoggerFactory.getLogger(ColchainGraphAssembler.class);
    private static boolean initialized;

    public static void init() {
        if(initialized) {
            return;
        }

        initialized = true;

        Assembler.general.implementWith(ColchainJenaConstants.COLCHAIN_GRAPH, new ColchainGraphAssembler());
    }

    @Override
    public Model open(Assembler a, Resource root, Mode mode)
    {
        try {
            ColchainGraph graph = new ColchainGraph();
            return ModelFactory.createModelForGraph(graph);
        } catch (Exception e) {
            log.error("Error creating graph: {}", e);
            throw new AssemblerException(root, "Error creating graph / "+e.toString());
        }
    }

    static {
        init();
    }
}
