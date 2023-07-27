package org.colchain.colchain.sparql.exceptions;

public class QueryInterruptedException extends RuntimeException {
    public QueryInterruptedException(Throwable cause) {
        super(cause);
    }

    public QueryInterruptedException(String message) {
        super(message);
    }
}
