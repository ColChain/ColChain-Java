package org.colchain.colchain.writer;

public class ResponseWriterFactory {
    public static IResponseWriter createWriter() {
        return new ResponseWriteHtml();
    }
}
