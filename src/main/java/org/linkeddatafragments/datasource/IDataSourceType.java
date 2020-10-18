package org.linkeddatafragments.datasource;

import org.linkeddatafragments.exceptions.DataSourceCreationException;

import com.google.gson.JsonObject;


public interface IDataSourceType
{
    /**
     * Creates a data source of this type.
     * 
     * @param title
     *        The title of the data source (as given in the config file).
     * 
     * @param description
     *        The description of the data source (as given in the config file).
     *
     * @param settings
     *        The properties of the data source to be created; usually, these
     *        properties are given in the config file of the LDF server. 
     * @return  
     * @throws org.linkeddatafragments.exceptions.DataSourceCreationException 
     */
    IDataSource createDataSource( final String title,
                                  final String description,
                                  final JsonObject settings )
                                                    throws DataSourceCreationException;
}
