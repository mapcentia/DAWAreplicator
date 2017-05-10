package com.mapcentia.aws_sync;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

/**
 * Created by mh on 14/10/16.
 */
final class Sekvens extends Stream {

    private String rel;

    Sekvens() {
        Configuration configuration = new Configuration();
        this.rel = configuration.getSchema() + "." + "sekvens";
    }

    /**
     * @throws Exception
     */
    SekvensObj get() throws Exception {
        try {
            this.createTabel();
        } catch (Exception e) {

        }

        HttpURLConnection con = this.start("http://dawa.aws.dk/replikering/senestesekvensnummer");
        String inputLine;
        StringBuffer response = new StringBuffer();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        // Deserialize JSON response
        Gson g = new Gson();
        SekvensObj sekvens = g.fromJson(response.toString(), SekvensObj.class);


        return sekvens;
    }

    /**
     * Package-private
     */
    class SekvensObj {
        Integer sekvensnummer;
        String tidspunkt;

        public SekvensObj(Integer sekvensnummer, String tidspunkt) {
            this.sekvensnummer = sekvensnummer;
            this.tidspunkt = tidspunkt;
        }
    }

    int getLastFromDb() throws Exception {
        String sql = "select * from " + rel + " ORDER BY id DESC limit 1";
        Connection c = Connect.open();
        Statement stmt = c.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        int val = rs.getInt("val");
        stmt.close();
        c.close();
        return val;
    }

    void storeInDb(int val) throws Exception {
        Connection c = Connect.open();
        PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + rel + "(val) VALUES(?)");
        pstmt.setInt(1, val);
        pstmt.executeUpdate();
        pstmt.close();
        c.close();
    }

    /**
     * @throws Exception
     */
    private void createTabel() throws Exception {
        String sql = "CREATE TABLE " + rel + " " +
                "(id   serial   PRIMARY KEY     NOT NULL, " +
                " val  int                      NOT NULL )";

        Connection c = Connect.open();
        Statement stmt = c.createStatement();
        stmt.executeUpdate(sql);

        PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + rel + "(val) VALUES(?)");
        pstmt.setInt(1, 0);
        pstmt.executeUpdate();

        stmt.close();
        c.close();
    }
}
