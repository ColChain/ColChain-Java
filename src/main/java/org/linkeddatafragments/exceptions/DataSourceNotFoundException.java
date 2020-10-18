package org.linkeddatafragments.exceptions;

 
public class DataSourceNotFoundException extends DataSourceException {

    /**
     *
     * @param dataSourceName
     */
    public DataSourceNotFoundException(String dataSourceName) {
        super(dataSourceName, "Datasource not found.");
    }  
}
