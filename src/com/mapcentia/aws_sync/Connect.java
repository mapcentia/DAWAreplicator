package com.mapcentia.aws_sync;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by mh on 26/10/16.
 */
public final class Connect {

    private static String url;
    private static String user;
    private static String pw;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPw(String pw) {
        this.pw = pw;
    }

    static Connection open() throws Exception {
        Connection c = DriverManager.getConnection(url, user, pw);
        return c;
    }
}

