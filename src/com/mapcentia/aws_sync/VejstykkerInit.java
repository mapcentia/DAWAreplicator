package com.mapcentia.aws_sync;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.*;
import java.util.Arrays;

/**
 * Created by mh on 24/10/16.
 */
final class VejstykkerInit extends Stream {

    /**
     * @param sekvensNummer
     * @throws Exception
     */
    void get(int sekvensNummer) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "vejstykker";

        try {
            this.createTabel(rel);
        } catch (Exception e) {

        }
        HttpURLConnection con = this.start("http://dawa.aws.dk/replikering/vejstykker?sekvensnummer=" + sekvensNummer + "&format=csv");
        String inputLine;
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

        Connect connect = new Connect();
        Connection c = connect.open();

        c.setAutoCommit(false);
        PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + rel + " VALUES(?, ?, ?, ?, ?, ?)");
        boolean first = true;
        int n;
        int lineCount = 0;

        while ((inputLine = in.readLine()) != null) {
            if (first) {
                first = false;
                continue;
            }
            n = 0;
            String[] arr = inputLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); // Spilt csv line
            System.out.print("\rIndsætter vejstykker... " + lineCount);
            System.out.flush();

            pstmt.setString(n + 1, arr[n]); // kode
            pstmt.setString(++n + 1, arr[n]); // kommunekode
            pstmt.setString(++n + 1, arr[n]); // navn
            pstmt.setString(++n + 1, arr[n]); // adresseringsnavn
            pstmt.setTimestamp(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // oprettet
            pstmt.setTimestamp(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // aendret
            pstmt.executeUpdate();
            lineCount++;
        }
        pstmt.close();
        c.commit();
        c.close();
        in.close();
    }

    /**
     *
     */
    private void createTabel(String rel) throws Exception {
        String sql = "CREATE TABLE " + rel + " " +
                "(kode              varchar(255)                   NOT NULL, " +
                " kommunekode       varchar(255)                   NOT NULL, " +
                " navn              varchar(255)                   NOT NULL, " +
                " adresseringsnavn  varchar(255)                           , " +
                " oprettet          timestamp                              , " +
                " aendret           timestamp                               )";

        Connect connect = new Connect();
        Connection c = connect.open();

        Statement stmt = c.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
        c.close();
    }
}
