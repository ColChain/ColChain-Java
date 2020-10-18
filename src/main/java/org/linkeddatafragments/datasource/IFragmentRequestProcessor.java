package org.linkeddatafragments.datasource;

import java.io.Closeable;

import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragment;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;
import org.linkeddatafragments.fragments.ILinkedDataFragmentRequest;


public interface IFragmentRequestProcessor extends Closeable
{

    /**
     *
     * @param request
     * @return
     * @throws IllegalArgumentException
     */
    ILinkedDataFragment createRequestedFragment(
            final ILinkedDataFragmentRequest request )
                    throws IllegalArgumentException;
}
