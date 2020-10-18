package org.linkeddatafragments.exceptions;

 
public class NoRegisteredMimeTypesException extends Exception {

    /**
     * Constructs the exception
     */
    public NoRegisteredMimeTypesException() {
        super("List of supported mimeTypes is empty.");
    }
    
}
