package org.colchain.colchain.sparql.solver;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;

public class ColchainEngine extends QueryEngineMain {
    protected Query colchainQuery;
    protected DatasetGraph colchainDataset;
    protected Binding colchainBinding;
    protected Context colchainContext;

    public ColchainEngine(Query query, DatasetGraph dataset, Binding input, Context context) {
        super(query, dataset, input, context);
        this.colchainQuery = query;
        this.colchainDataset = dataset;
        this.colchainBinding = input;
        this.colchainContext = context;
    }

    public ColchainEngine(Op op, DatasetGraph dataset, Binding input, Context context) {
        super(op, dataset, input, context);
    }

    // ---- Registration of the factory for this query engine class.

    // Query engine factory.
    // Call PiqnicExtEngine.register() to add to the global query engine registry.

    static QueryEngineFactory factory = new ColchainEngine.ColchainEngineFactory();

    static public QueryEngineFactory getFactory() {
        return factory;
    }

    static public void register() {
        QueryEngineRegistry.addFactory(factory);
    }

    static public void unregister() {
        QueryEngineRegistry.removeFactory(factory);
    }

    static class ColchainEngineFactory implements QueryEngineFactory {

        @Override
        public boolean accept(Query query, DatasetGraph dataset, Context context) {
            return true;
        }

        @Override
        public Plan create(Query query, DatasetGraph dataset, Binding initial, Context context) {
            ColchainEngine engine = new ColchainEngine(query, dataset, initial, context);
            return engine.getPlan();
        }

        @Override
        public boolean accept(Op op, DatasetGraph dataset, Context context) {
            // Refuse to accept algebra expressions directly.
            return false;
        }

        @Override
        public Plan create(Op op, DatasetGraph dataset, Binding inputBinding, Context context) {
            // Should not be called because accept/Op is false
            throw new ARQInternalErrorException("ColchainQueryEngine: factory called directly with an algebra expression");
        }
    }
}
