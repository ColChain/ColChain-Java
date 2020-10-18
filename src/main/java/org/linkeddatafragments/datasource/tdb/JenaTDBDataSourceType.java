package org.linkeddatafragments.datasource.tdb;

import java.io.File;

import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IDataSourceType;
import org.linkeddatafragments.exceptions.DataSourceCreationException;

import com.google.gson.JsonObject;

 
public class JenaTDBDataSourceType implements IDataSourceType
{
    @Override
    public IDataSource createDataSource( final String title,
                                         final String description,
                                         final JsonObject settings )
                                                     throws DataSourceCreationException
    {
        final String dname = settings.getAsJsonPrimitive("directory").getAsString();
        final File dir = new File( dname );

        try {
            return new JenaTDBDataSource(title, description, dir);
        } catch (Exception ex) {
            throw new DataSourceCreationException(ex);
        }
    }

}
