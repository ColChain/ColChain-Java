package org.linkeddatafragments.fragments.tpf;

import org.linkeddatafragments.fragments.ILinkedDataFragment;
 
public interface ITriplePatternFragment extends ILinkedDataFragment {
    /**
     * Gets the total number of triples in the fragment (can be an estimate).
     * @return the total number of triples
     */
    public long getTotalSize();
}
