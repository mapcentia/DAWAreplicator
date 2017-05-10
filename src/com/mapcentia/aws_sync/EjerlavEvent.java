package com.mapcentia.aws_sync;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created by mh on 14/10/16.
 */
final class EjerlavEvent extends Stream {

    /**
     *
     * @param sekvensNummerFra
     * @param sekvensNummerTil
     * @return
     * @throws Exception
     */
    EjerlavObj[] get(int sekvensNummerFra, int sekvensNummerTil, Connection c) throws Exception {
        String url = "http://dawa.aws.dk/replikering/ejerlav/haendelser?sekvensnummerfra=" + sekvensNummerFra +"&sekvensnummertil=" + sekvensNummerTil;
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
        EjerlavObj[] Postnumre = g.fromJson(response.toString(), EjerlavObj[].class);

        // Prepare statements
        // ==================

        PreparedStatement pstmtInsert = c.prepareStatement("INSERT INTO replika.ejerlav VALUES(?, ?)");
        PreparedStatement pstmtUpdate = c.prepareStatement("UPDATE replika.ejerlav SET navn=? WHERE kode=?");
        PreparedStatement pstmtDelete = c.prepareStatement("DELETE FROM replika.ejerlav WHERE kode=?");

        // Execute
        // =======

        for (EjerlavEvent.EjerlavObj item : Postnumre) {
            int n = 0;

            switch (item.operation) {
                case "insert":
                    pstmtInsert.setString(n + 1, item.data.kode); // kode
                    pstmtInsert.setString(++n + 1, item.data.navn); // navn
                    pstmtInsert.executeUpdate();
                    cInsert++;
                    break;
                case "update":
                    pstmtInsert.setString(n + 1, item.data.navn); // navn
                    pstmtInsert.setString(++n + 1, item.data.kode); // kode
                    cUpdate++;
                    break;
                case "delete":
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

        return Postnumre;
    }

    /**
     *
     */
    final class EjerlavObj {
        /**
         *
         */
        class DataObj {
            String kode;
            String navn;
        }
        String operation;
        String tidspunkt;
        Integer sekvensnummer;
        DataObj data;

        /**
         *
         * @param operation
         * @param tidspunkt
         * @param sekvensnummer
         * @param data
         */
        public EjerlavObj(String operation, String tidspunkt, Integer sekvensnummer, DataObj data) {
            this.operation = operation;
            this.tidspunkt = tidspunkt;
            this.sekvensnummer = sekvensnummer;
            this.data = data;
        }
    }
}