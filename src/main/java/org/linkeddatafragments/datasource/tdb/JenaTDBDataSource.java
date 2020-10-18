package org.linkeddatafragments.datasource.tdb;

import java.io.File;
import java.util.Set;

import org.colchain.index.ppbf.IBloomFilter;
import org.colchain.index.ppbf.impl.PrefixPartitionedBloomFilter;
import org.colchain.index.util.Triple;
import org.colchain.colchain.transaction.ITransaction;
import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;
import org.linkeddatafragments.fragments.tpf.TPFRequestParserForJenaBackends;
import org.rdfhdt.hdt.hdt.HDT;


public class JenaTDBDataSource extends DataSourceBase {

    /**
     * The request processor
     *
     */
    protected final JenaTDBBasedRequestProcessorForTPFs requestProcessor;

    @Override
    public IFragmentRequestParser getRequestParser(IDataSource.ProcessorType processor)
    {
        return TPFRequestParserForJenaBackends.getInstance();
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor(IDataSource.ProcessorType processor)
    {
        return requestProcessor;
    }

    @Override
    public HDT getHdt() {
        return null;
    }

    @Override
    public void updateHdt(ITransaction transaction) {

    }

    @Override
    public void deleteBloomFilter() {

    }

    @Override
    public void copy() {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void restore() {

    }

    @Override
    public IDataSource materializeVersion(Set<Triple> additions, Set<Triple> deletions, long timestamp) {
        return null;
    }

    /**
     * Constructor
     *
     * @param title
     * @param description
     * @param tdbdir directory used for TDB backing
     */
    public JenaTDBDataSource(String title, String description, File tdbdir) {
        super(title, description);
        requestProcessor = new JenaTDBBasedRequestProcessorForTPFs( tdbdir );
    }

    @Override
    public IBloomFilter<String> createBloomFilter() {
        return PrefixPartitionedBloomFilter.create("empty.ppbf");
    }

    @Override
    public void remove() {

    }
}
