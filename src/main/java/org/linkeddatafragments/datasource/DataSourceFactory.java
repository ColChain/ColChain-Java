package org.linkeddatafragments.datasource;

import com.google.gson.JsonObject;
import org.linkeddatafragments.datasource.hdt.HdtDataSource;
import org.linkeddatafragments.exceptions.DataSourceCreationException;
import org.linkeddatafragments.exceptions.UnknownDataSourceTypeException;

import java.io.IOException;


public class DataSourceFactory {
    /**
     * Create a datasource using a JSON config
     *
     * @param config
     * @return datasource interface
     * @throws DataSourceCreationException
     */
    public static IDataSource create(JsonObject config) throws DataSourceCreationException {
        String title = config.getAsJsonPrimitive("title").getAsString();
        String description = config.getAsJsonPrimitive("description").getAsString();
        String typeName = config.getAsJsonPrimitive("type").getAsString();

        JsonObject settings = config.getAsJsonObject("settings");

        final IDataSourceType type = DataSourceTypesRegistry.getType(typeName);
        if ( type == null )
            throw new UnknownDataSourceTypeException(typeName);

        return type.createDataSource( title, description, settings );
    }

    public static IDataSource createLocal(String id, String path) throws IOException {
        return new HdtDataSource(id, "HDT file with id "+id, path);
    }

}
