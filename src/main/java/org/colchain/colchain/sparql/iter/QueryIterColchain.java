package org.colchain.colchain.sparql.iter;

import org.colchain.index.graph.IGraph;
import org.colchain.colchain.sparql.ColchainBindings;
import org.colchain.colchain.sparql.ColchainJenaConstants;
import org.colchain.colchain.sparql.exceptions.QueryInterruptedException;
import org.colchain.colchain.sparql.graph.ColchainGraph;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.util.iterator.ExtendedIterator;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class QueryIterColchain extends QueryIterRepeatApply {
    private final Triple pattern;
    int count = 0;
    private QueryIterator currentStage = null;
    private volatile boolean cancelRequested = false;
    private Set<IGraph> fragments;

    public QueryIterColchain(QueryIterator input, Triple pattern, ExecutionContext cxt, Set<IGraph> fragments) {
        super(input, cxt);
        this.pattern = pattern;

        this.fragments = fragments;
        if(fragments == null) this.fragments = new HashSet<>();
    }

    protected QueryIterator nextStage(Binding binding) {
        return null;
    }

    protected QueryIterator nextStage(ColchainBindings bindings) {
        return new BindMapper(bindings, this.pattern, this.getExecContext(), fragments);
    }

    @Override
    protected boolean hasNextBinding() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (this.isFinished()) {
            return false;
        } else {
            while(true) {
                if (this.currentStage == null) {
                    this.currentStage = this.makeNextStage();
                }

                if (this.currentStage == null) {
                    return false;
                }

                if (this.cancelRequested) {
                    performRequestCancel(this.currentStage);
                }

                if (this.currentStage.hasNext()) {
                    return true;
                }

                this.currentStage.close();
                this.currentStage = null;
            }
        }
    }

    @Override
    protected Binding moveToNextBinding() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!this.hasNextBinding()) {
            throw new NoSuchElementException(Lib.className(this) + ".next()/finished");
        } else {
            return this.currentStage.nextBinding();
        }
    }

    private QueryIterator makeNextStage() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        ++this.count;
        if (this.getInput() == null) {
            return null;
        } else {
            ColchainBindings bindings = new ColchainBindings();
            for(int i = 0; i < ColchainJenaConstants.BIND_NUM; i++) {
                if(!this.getInput().hasNext()) break;
                Binding b = this.getInput().next();
                bindings.add(b);
            }

            if(bindings.size() == 0) {
                this.getInput().close();
                return null;
            }

            return nextStage(bindings);
        }
    }

    static class BindMapper extends QueryIter {
        private Node s;
        private Node p;
        private Node o;
        private Binding slot = null;
        private boolean finished = false;
        private volatile boolean cancelled = false;
        private ExtendedIterator<Pair<Triple, Binding>> iter;

        BindMapper(ColchainBindings bindings, Triple pattern, ExecutionContext cxt, Set<IGraph> fragments) {
            super(cxt);
            this.s = pattern.getSubject();
            this.p = pattern.getPredicate();
            this.o = pattern.getObject();

            ColchainGraph g = (ColchainGraph) cxt.getActiveGraph();
            iter = g.graphBaseFind(pattern, bindings, fragments);
        }

        private Binding mapper(Triple r, Binding b) {
            BindingMap results = BindingFactory.create(b);
            if (!insert(this.s, r.getSubject(), results)) {
                return null;
            } else if (!insert(this.p, r.getPredicate(), results)) {
                return null;
            } else {
                return !insert(this.o, r.getObject(), results) ? null : results;
            }
        }

        private static boolean insert(Node inputNode, Node outputNode, BindingMap results) {
            if (!Var.isVar(inputNode)) {
                return true;
            } else {
                Var v = Var.alloc(inputNode);
                Node x = results.get(v);
                if (x != null) {
                    return outputNode.equals(x);
                } else {
                    results.add(v, outputNode);
                    return true;
                }
            }
        }

        protected boolean hasNextBinding() {
            if(Thread.interrupted())
                throw new QueryInterruptedException("Interrupted.");
            if (this.finished) {
                return false;
            } else if (this.slot != null) {
                return true;
            } else if (this.cancelled) {
                this.finished = true;
                return false;
            } else {
                while(this.iter.hasNext() && this.slot == null) {
                    Pair<Triple, Binding> pair = this.iter.next();
                    Binding b = this.mapper(pair.car(), pair.cdr());
                    this.slot = b;
                }

                if (this.slot == null) {
                    this.finished = true;
                }

                return this.slot != null;
            }
        }

        protected Binding moveToNextBinding() {
            if(Thread.interrupted())
                throw new QueryInterruptedException("Interrupted.");
            if (!this.hasNextBinding()) {
                throw new ARQInternalErrorException();
            } else {
                Binding r = this.slot;
                this.slot = null;
                return r;
            }
        }

        protected void closeIterator() {
            /* Not Implemented. */
        }

        protected void requestCancel() {
            this.cancelled = true;
        }
    }
}
