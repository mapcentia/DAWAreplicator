package com.mapcentia.aws_sync;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.*;
import java.util.Arrays;
import java.util.UUID;

import org.postgis.*;

/**
 * Created by mh on 24/10/16.
 */
final class AdgangsAdresserInit extends Stream {
    /**
     * @param sekvensNummer
     * @throws Exception
     */
    void get(int sekvensNummer) throws Exception {

        try {
            this.createTabel();
        } catch (Exception e) {

        }

        HttpURLConnection con = this.start("http://dawa.aws.dk/replikering/adgangsadresser?sekvensnummertil=" + sekvensNummer + "&format=csv");
        String inputLine;
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

        Connection c = Connect.open();
        c.setAutoCommit(false);
        PreparedStatement pstmt = c.prepareStatement("INSERT INTO replika.adgangsadresser VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
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
            System.out.print("\rIndsætter adgangsadresser... " + lineCount);
            System.out.flush();

            pstmt.setObject(n + 1, UUID.fromString(arr[n])); // id
            pstmt.setInt(++n + 1, Integer.valueOf(arr[n])); // status
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // kommunekode
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // vejkode
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // husnr
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // supplerendebynavn
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // postnr
            pstmt.setTimestamp(++n + 1, Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", ""))); // oprettet
            pstmt.setTimestamp(++n + 1, Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", ""))); // aendret
            pstmt.setTimestamp(++n + 1, (arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // ikrafttraedelsesdato
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // ejerlavkode
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // matrikelnr
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // esrejendomsnr
            pstmt.setFloat(++n + 1, (arr[n].length() > 0) ? Float.valueOf(arr[n]) : 0); // etrs89koordinat_oest
            pstmt.setFloat(++n + 1, (arr[n].length() > 0) ? Float.valueOf(arr[n]) : 0); // etrs89koordinat_nord
            pstmt.setFloat(++n + 1, (arr[n].length() > 0) ? Float.valueOf(arr[n]) : 0); // hoejde
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // noejagtighed
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // kilde
            pstmt.setString(++n + 1, (arr[n].length() > 0) ? arr[n] : null); // husnummerkilde

            // Start to get out of bound and we check length of array
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // tekniskstandard
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // tekstretning
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // esdhreference
            pstmt.setString(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? arr[n] : null); // journalnummer
            pstmt.setTimestamp(++n + 1, ((arr.length > n) && arr[n].length() > 0) ? Timestamp.valueOf(arr[n].replace("T", " ").replace("Z", "")) : null); // adressepunktaendringsdato

            // Create geometry
            if (arr[13].length() > 0 && arr[14].length() > 0) {
                Point point = new Point();
                point.setX(Float.valueOf(arr[13]));
                point.setY(Float.valueOf(arr[14]));
                point.setSrid(25832);
                PGgeometry geom = new PGgeometry(point);
                pstmt.setObject(++n + 1, geom); // the_geom
            } else {
                pstmt.setObject(++n + 1, null); // the_geom
            }
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
    private void createTabel() throws Exception {
        String sql = "CREATE TABLE replika.adgangsadresser " +
                "(id                        uuid            PRIMARY KEY     NOT NULL, " +
                " status                    int                                     , " +
                " kommunekode               varchar(255)                            , " +
                " vejkode                   varchar(255)                            , " +
                " husnr                     varchar(255)                            , " +
                " supplerendebynavn         varchar(255)                            , " +
                " postnr                    varchar(255)                            , " +
                " oprettet                  timestamp                               , " +
                " aendret                   timestamp                               , " +
                " ikrafttraedelsesdato      timestamp                               , " +
                " ejerlavkode               varchar(255)                            , " +
                " matrikelnr                varchar(255)                            , " +
                " esrejendomsnr             varchar(255)                            , " +
                " etrs89koordinat_oest      float                                   , " +
                " etrs89koordinat_nord      float                                   , " +
                " hoejde                    float                                   , " +
                " noejagtighed              varchar(255)                            , " +
                " kilde                     varchar(255)                            , " +
                " husnummerkilde            varchar(255)                            , " +
                " tekniskstandard           varchar(255)                            , " +
                " tekstretning              varchar(255)                            , " +
                " esdhreference             varchar(255)                            , " +
                " journalnummer             varchar(255)                            , " +
                " adressepunktaendringsdato timestamp                               , " +
                " the_geom                  geometry('Point', 25832)                 )";

        Connection c = Connect.open();
        Statement stmt = c.createStatement();
        stmt.executeUpdate(sql);
        stmt.close();
        c.close();
    }
}
