package com.mapcentia.aws_sync;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by mh on 26/10/16.
 */
public final class Connect {

    private static String url;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    static Connection open() throws Exception {
        Connection c = DriverManager.getConnection(url, "gc2", "1234");
        return c;
    }
}

