package org.linkeddatafragments.fragments;

import jakarta.servlet.http.HttpServletRequest;

import org.linkeddatafragments.config.ConfigReader;

 
public interface IFragmentRequestParser
{
    /**
     * Parses the given HTTP request into a specific
     * {@link ILinkedDataFragmentRequest}.
     *
     * @param httpRequest
     * @param config
     * @return 
     * @throws IllegalArgumentException
     *         If the given HTTP request cannot be interpreted (perhaps due to
     *         missing request parameters).  
     */
    ILinkedDataFragmentRequest parseIntoFragmentRequest(
            final HttpServletRequest httpRequest,
            final ConfigReader config )
                    throws IllegalArgumentException;
}
