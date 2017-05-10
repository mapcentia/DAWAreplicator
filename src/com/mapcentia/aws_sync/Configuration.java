package com.mapcentia.aws_sync;

/**
 * Created by mh on 10/05/17.
 */

import static java.lang.String.format;

public final class Configuration {

    private Connect connection;
    private static String schema;


    public void setConnection(Connect connection) {
        this.connection = connection;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }


    @Override
    public String toString() {
        return new StringBuilder()
                .append(format("Connecting to database: %s\n", connection.getUrl()))
                .toString();
    }
}

