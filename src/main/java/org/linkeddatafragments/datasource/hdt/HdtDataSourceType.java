package org.linkeddatafragments.datasource.hdt;

import java.io.File;
import java.io.IOException;

import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IDataSourceType;
import org.linkeddatafragments.exceptions.DataSourceCreationException;

import com.google.gson.JsonObject;

 
public class HdtDataSourceType implements IDataSourceType
{
    @Override
    public IDataSource createDataSource( final String title,
                                         final String description,
                                         final JsonObject settings )
                                                     throws DataSourceCreationException
    {
        final String fname = settings.getAsJsonPrimitive("file").getAsString();
        final File file = new File( fname );
        
        try {
            return new HdtDataSource(title, description, file.getAbsolutePath());
        } catch (IOException ex) {
            throw new DataSourceCreationException(ex);
        }
    }

}
