package org.colchain.colchain.writer;


import org.colchain.index.util.Tuple;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import java.util.Set;


public interface IResponseWriter {
    /**
     * Serializes and writes not initiated message
     *
     * @param outputStream The response stream to write to
     * @param request Parsed request for fragment
     * @throws Exception Error that occurs while serializing
     */
    void writeNotInitiated(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeLandingPage(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeRedirect(ServletOutputStream outputStream, HttpServletRequest request, String path) throws Exception;

    void writeQueryResults(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeSearch(ServletOutputStream outputStream, HttpServletRequest request, Set<Tuple<String, Tuple<String, String>>> comms) throws Exception;

    void writeInit(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeSuggestUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;

    void writeSuggestedUpdate(ServletOutputStream outputStream, HttpServletRequest request) throws Exception;
}
