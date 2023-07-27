package org.linkeddatafragments.fragments.tpf;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.linkeddatafragments.fragments.LinkedDataFragmentRequestBase;

import java.util.List;

public class BindingsRestrictedTriplePatternFragmentRequestImpl<CTT, NVT, AVT>
        extends LinkedDataFragmentRequestBase
        implements ITriplePatternFragmentRequest<CTT, NVT, AVT> {
    /**
     *
     */
    public final ITriplePatternElement<CTT, NVT, AVT> subject;

    /**
     *
     */
    public final ITriplePatternElement<CTT, NVT, AVT> predicate;

    /**
     *
     */
    public final ITriplePatternElement<CTT, NVT, AVT> object;

    public final List<Binding> bindings;
    public final List<Var> vars;

    /**
     * @param fragmentURL
     * @param datasetURL
     * @param pageNumberWasRequested
     * @param pageNumber
     * @param subject
     * @param predicate
     * @param object
     */
    public BindingsRestrictedTriplePatternFragmentRequestImpl(final String fragmentURL,
                                                              final String datasetURL,
                                                              final boolean pageNumberWasRequested,
                                                              final long pageNumber,
                                                              final ITriplePatternElement<CTT, NVT, AVT> subject,
                                                              final ITriplePatternElement<CTT, NVT, AVT> predicate,
                                                              final ITriplePatternElement<CTT, NVT, AVT> object,
                                                              final List<Binding> bindings,
                                                              final List<Var> vars) {
        super(fragmentURL, datasetURL, pageNumberWasRequested, pageNumber);

        if (subject == null)
            throw new IllegalArgumentException();

        if (predicate == null)
            throw new IllegalArgumentException();

        if (object == null)
            throw new IllegalArgumentException();

        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
        this.bindings = bindings;
        this.vars = vars;
    }

    @Override
    public ITriplePatternElement<CTT, NVT, AVT> getSubject() {
        return subject;
    }

    @Override
    public ITriplePatternElement<CTT, NVT, AVT> getPredicate() {
        return predicate;
    }

    @Override
    public ITriplePatternElement<CTT, NVT, AVT> getObject() {
        return object;
    }

    public List<Binding> getBindings() {
        return bindings;
    }

    public List<Var> getVars() {
        return vars;
    }

    @Override
    public String toString() {
        return "TriplePatternFragmentRequest(" +
                "class: " + getClass().getName() +
                ", subject: " + subject.toString() +
                ", predicate: " + predicate.toString() +
                ", object: " + object.toString() +
                ", bindings: " + bindings.toString() +
                ", vars: " + vars.toString() +
                ", fragmentURL: " + fragmentURL +
                ", isPageRequest: " + pageNumberWasRequested +
                ", pageNumber: " + pageNumber +
                ")";
    }
}
