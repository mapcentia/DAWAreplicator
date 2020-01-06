package com.mapcentia.aws_sync;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * Created by mh on 27/10/16.
 */
final class AdresserInit extends Stream {
    /**
     *
     * @param sekvensNummer
     * @throws Exception
     */
    void get(int sekvensNummer) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "adresser";

        try {
            this.createTabel(rel);
        } catch (Exception e) {

        }

        HttpURLConnection con = this.start("http://dawa.aws.dk/replikering/adresser?sekvensnummer=" + sekvensNummer + "&format=csv");
        String inputLine;
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8));

        Connection c = Connect.open();
        c.setAutoCommit(false);
        PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + rel + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        boolean first = true;
        int n;
        int lineCount = 0;

        while ((inputLine = in.readLine()) != null) {
            if (first) {
//              System.out.print(inputLine + "\n");
                first = false;
                continue;
            }
            n = 0;
            String[] arr = inputLine.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            System.out.print("\rIndsætter adresser... " + lineCount);
            System.out.flush();

            pstmt.setObject(n + 1, UUID.fromString(arr[n]), Types.OTHER); // id
            pstmt.setInt(++n + 1, Integer.valueOf(arr[n])); // status
            pstmt.setTimestamp(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // oprettet
            pstmt.setTimestamp(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // aendret
            pstmt.setTimestamp(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // ikrafttraedelsesdato
            pstmt.setObject(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? UUID.fromString(arr[n]) : null, Types.OTHER); // adgangsadresseid
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // etage
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // doer
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // kilde
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // esdhreference
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // journalnummer

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
                " (id                   UUID           PRIMARY KEY      NOT NULL, " +
                " status                int                                     , " +
                " oprettet              timestamp                               , " +
                " aendret               timestamp                               , " +
                " ikrafttraedelsesdato  timestamp                               , " +
                " adgangsadresseid      uuid                                    , " +
                " etage                 varchar(255)                            , " +
                " doer                  varchar(255)                            , " +
                " kilde                 varchar(255)                            , " +
                " esdhreference         varchar(255)                            , " +
                " journalnummer         varchar(255)                             )";

        Connection c = Connect.open();
        Statement stmt = c.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
        c.close();
    }
}
