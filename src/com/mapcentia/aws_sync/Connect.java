package com.mapcentia.aws_sync;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by mh on 26/10/16.
 */
class Connect {
    static Connection open() throws Exception{
        Connection c = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/mydb", "gc2", "1234");
        return c;
    }
}

