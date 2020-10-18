package org.linkeddatafragments.exceptions;

 
public class UnknownDataSourceTypeException extends DataSourceCreationException {
    
    /**
     *
     * @param type
     */
    public UnknownDataSourceTypeException(String type) {
        super("", "Type " + type + " does not exist.");
    } 
}
