package com.mapcentia.aws_sync;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

/**
 * Created by mh on 03/02/17.
 */
final class VejstykkerEvent extends Stream {

    /**
     *
     * @param sekvensNummerFra
     * @param sekvensNummerTil
     * @param c
     * @return
     * @throws Exception
     */
    VejstykkerObj[] get(int sekvensNummerFra, int sekvensNummerTil, Connection c) throws Exception {
        String url = "http://dawa.aws.dk/replikering/vejstykker/haendelser?sekvensnummerfra=" + (sekvensNummerFra +1) + "&sekvensnummertil=" + sekvensNummerTil;
        System.out.println(url);

        HttpURLConnection con = this.start(url);
        String inputLine;
        StringBuffer response = new StringBuffer();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        // Set counters
        // ============
        int cInsert = 0;
        int cUpdate = 0;
        int cDelete = 0;

        // Deserialize JSON response
        // =========================

        Gson g = new Gson();
        //System.out.println(response.toString());
        VejstykkerEvent.VejstykkerObj[] Adresser = g.fromJson(response.toString(), VejstykkerEvent.VejstykkerObj[].class);

        // Prepare statements
        // ==================

        PreparedStatement pstmtInsert = c.prepareStatement("INSERT INTO replika.vejstykker VALUES(?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtUpdate = c.prepareStatement("UPDATE replika.vejstykker SET kommunekode=?, navn=?, adresseringsnavn=?, oprettet=?, aendret=? WHERE kode=?");
        PreparedStatement pstmtDelete = c.prepareStatement("DELETE FROM replika.vejstykker WHERE kode=?");

        // Execute
        // =======

        for (VejstykkerEvent.VejstykkerObj item : Adresser) {
            int n = 0;
            switch (item.operation) {
                case "insert":
                    //System.out.println(item.operation);
                    pstmtInsert.setString(n + 1, item.data.kode); // kode
                    pstmtInsert.setString(++n + 1, item.data.kommunekode); // kommunekode
                    pstmtInsert.setString(++n + 1, item.data.navn); // navn
                    pstmtInsert.setString(++n + 1, item.data.adresseringsnavn); // adresseringsnavn
                    pstmtInsert.setTimestamp(++n + 1, (item.data.oprettet != null) ? java.sql.Timestamp.valueOf(item.data.oprettet.replace("T", " ").replace("Z", "")) : null); // oprettet
                    pstmtInsert.setTimestamp(++n + 1, (item.data.ændret != null) ? java.sql.Timestamp.valueOf(item.data.ændret.replace("T", " ").replace("Z", "")) : null); // aendret
                    pstmtInsert.executeUpdate();
                    cInsert++;
                    break;
                case "update":
                    //System.out.println(item.operation);
                    pstmtUpdate.setString(n + 1, item.data.kommunekode); // kommunekode
                    pstmtUpdate.setString(++n + 1, item.data.navn); // navn
                    pstmtUpdate.setString(++n + 1, item.data.adresseringsnavn); // adresseringsnavn
                    pstmtUpdate.setTimestamp(++n + 1, (item.data.oprettet != null) ? java.sql.Timestamp.valueOf(item.data.oprettet.replace("T", " ").replace("Z", "")) : null); // oprettet
                    pstmtUpdate.setTimestamp(++n + 1, (item.data.ændret != null) ? java.sql.Timestamp.valueOf(item.data.ændret.replace("T", " ").replace("Z", "")) : null); // aendret
                    pstmtUpdate.setString(++n + 1, item.data.kode); // kode
                    cUpdate++;
                    pstmtUpdate.executeUpdate();
                    break;
                case "delete":
                    //System.out.println(item.data);
                    pstmtDelete.setString(1, item.data.kode); // kode
                    pstmtDelete.executeUpdate();
                    cDelete++;
                    break;
            }
            System.out.print("\rInsert: " + cInsert);
            System.out.print(", Update: " + cUpdate);
            System.out.print(", Delete: " + cDelete);
            System.out.flush();
        }
        System.out.print("\n");

        // Close prepared statements
        pstmtDelete.close();
        pstmtInsert.close();
        pstmtUpdate.close();

        return Adresser;
    }

    /**
     *
     */
    final class VejstykkerObj {
        /**
         *
         */
        class DataObj {
            String kode;
            String kommunekode;
            String navn;
            String adresseringsnavn;
            String oprettet;
            String ændret;
        }
        String operation;
        String tidspunkt;
        Integer sekvensnummer;
        VejstykkerEvent.VejstykkerObj.DataObj data;

        /**
         *
         * @param operation
         * @param tidspunkt
         * @param sekvensnummer
         * @param data
         */
        public VejstykkerObj(String operation, String tidspunkt, Integer sekvensnummer, VejstykkerEvent.VejstykkerObj.DataObj data) {
            this.operation = operation;
            this.tidspunkt = tidspunkt;
            this.sekvensnummer = sekvensnummer;
            this.data = data;
        }
    }
}
