package org.linkeddatafragments.datasource;

import java.io.Closeable;

import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.util.Triple;
import org.colchain.colchain.transaction.ITransaction;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.rdfhdt.hdt.hdt.HDT;

import java.util.Set;

public interface IDataSource extends Closeable {

    /**
     *
     * @return
     */
    public String getTitle();

    /**
     *
     * @return
     */
    public String getDescription();

    /**
     * Returns a data source specific {@link IFragmentRequestParser}.
     * @return
     */
    IFragmentRequestParser getRequestParser(ProcessorType processor);

    /**
     * Returns a data source specific {@link IFragmentRequestProcessor}.
     * @return
     */
    IFragmentRequestProcessor getRequestProcessor(ProcessorType processor);

    HDT getHdt();

    IBloomFilter<String> createBloomFilter();

    void updateHdt(ITransaction transaction);

    void remove();

    void deleteBloomFilter();

    void copy();

    long size();

    void restore();

    IDataSource materializeVersion(Set<Triple> additions, Set<Triple> deletions, long timestamp);

    int numTriples();

    int numSubjects();

    int numPredicates();

    int numObjects();

    public enum ProcessorType {
        TPF, BRTPF, SPF
    }
}
