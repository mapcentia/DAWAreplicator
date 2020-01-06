package com.mapcentia.aws_sync;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.UUID;
import java.nio.charset.StandardCharsets;


/**
 * Created by mh on 24/10/16.
 */
final class EjerlavInit extends Stream {

    /**
     * @param sekvensNummer
     * @throws Exception
     */
    void get(int sekvensNummer) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "ejerlav";

        try {
            this.createTabel(rel);
        } catch (Exception e) {

        }

        HttpURLConnection con = this.start("http://dawa.aws.dk/replikering/ejerlav?sekvensnummer=" + sekvensNummer + "&format=csv");
        String inputLine;
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8));

        Connection c = Connect.open();
        c.setAutoCommit(false);
        PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + rel + " VALUES(?, ?)");
        boolean first = true;
        int n;
        int lineCount = 0;

        while ((inputLine = in.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            n = 0;
            String[] arr = inputLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            System.out.print("\rIndsætter ejerlav... " + lineCount);
            System.out.flush();
            pstmt.setString(n + 1, arr[n]); // kode
            pstmt.setString(++n + 1, arr[n].replace("\"", "")); // navn

            pstmt.executeUpdate();
            lineCount++;
        }
        pstmt.close();
        c.commit();
        c.close();
        in.close();
    }

    /**
     * @throws Exception
     */
    private void createTabel(String rel) throws Exception {
        String sql = "CREATE TABLE " + rel + " " +
                "(kode      varchar(255)        PRIMARY KEY     NOT NULL, " +
                " navn      varchar(255)                                 )";

        Connection c = Connect.open();
        Statement stmt = c.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
        c.close();
    }
}
