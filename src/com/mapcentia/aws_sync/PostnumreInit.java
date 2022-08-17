package com.mapcentia.aws_sync;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

/**
 * Created by mh on 14/10/16.
 */
final class PostnumreInit extends Stream {

    /**
     * @param sekvensNummer
     * @throws Exception
     */
    void get(int sekvensNummer) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "postnumre";

        try {
            this.createTabel(rel);
        } catch (Exception e) {

        }
        //System.exit(0);
        HttpURLConnection con = this.start("https://dawa.aws.dk/replikering/postnumre?sekvensnummer=" + sekvensNummer + "&format=csv");
        String inputLine;
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8));

        Connect connect = new Connect();
        Connection c = connect.open();

        c.setAutoCommit(false);
        PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + rel + " VALUES(?, ?, ?)");
        boolean first = true;
        int n = 0;
        int lineCount = 0;

        while ((inputLine = in.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            n = 0;
            String[] arr = inputLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            System.out.print("\rIndsætter postnumre... " + lineCount);
            System.out.flush();

            pstmt.setString(n + 1, arr[n]); // kode
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // kommunekode
            pstmt.setBoolean(++n + 1, (arr.length > n) && (arr[n] != "1"));
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
                "(nr                varchar(255)   PRIMARY KEY     NOT NULL, " +
                " navn              varchar(255)                   NOT NULL, " +
                " stormodtager      bool                                    )";

        Connect connect = new Connect();
        Connection c = connect.open();

        Statement stmt = c.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
        c.close();
    }
}