package org.linkeddatafragments.datasource;

import java.util.HashMap;
import java.util.Map;


public class DataSourceTypesRegistry
{
    private static Map<String, IDataSourceType> registry =
                                        new HashMap<String, IDataSourceType>();

    /**
     *
     * @param typeName
     * @return
     */
    public static synchronized IDataSourceType getType( final String typeName )
    {
        return registry.get( typeName );
    }

    /**
     *
     * @param typeName
     * @return
     */
    public static synchronized boolean isRegistered( final String typeName )
    {
        return registry.containsKey( typeName );
    }

    /**
     *
     * @param typeName
     * @param type
     */
    public static synchronized void register( final String typeName,
                                              final IDataSourceType type )
    {
        if ( registry.containsKey(typeName) ) {
            throw new IllegalArgumentException( "The registry already " +
                       "contains a type with the name '" + typeName + "'." );
        }
        registry.put( typeName, type );
    }

}
