package com.mapcentia.aws_sync;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Created by mh on 14/10/16.
 */
final class PostnumreEvent extends Stream {

    /**
     *
     * @param sekvensNummerFra
     * @param sekvensNummerTil
     * @param c
     * @return
     * @throws Exception
     */
    PostnumreObj[] get(int sekvensNummerFra, int sekvensNummerTil, Connection c) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "postnumre";

        String url = "http://dawa.aws.dk/replikering/postnumre/haendelser?sekvensnummerfra=" + sekvensNummerFra + "&sekvensnummertil=" + sekvensNummerTil;
        System.out.println(url);

        HttpURLConnection con = this.start(url);
        String inputLine;
        StringBuffer response = new StringBuffer();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8));

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
        PostnumreObj[] Postnumre = g.fromJson(response.toString(), PostnumreObj[].class);

        // Prepare statements
        // ==================

        PreparedStatement pstmtInsert = c.prepareStatement("INSERT INTO " + rel + " (nr,navn,stormodtager) VALUES(?, ?, ?)");
        PreparedStatement pstmtUpdate = c.prepareStatement("UPDATE " + rel + " SET nr=?, navn=?, stormodtager=? WHERE nr=?");
        PreparedStatement pstmtDelete = c.prepareStatement("DELETE FROM  " + rel + " WHERE nr=?");

        // Execute
        // =======

        for (PostnumreEvent.PostnumreObj item : Postnumre) {
            int n = 0;

            switch (item.operation) {
                case "insert":
                    pstmtInsert.setString(n + 1, item.data.nr); // nr
                    pstmtInsert.setString(++n + 1, item.data.navn); // navn
                    pstmtInsert.setBoolean(++n + 1, Boolean.valueOf(item.data.stormodtager)); // stormodtager
                    pstmtInsert.executeUpdate();
                    cInsert++;
                    break;
                case "update":
                    pstmtUpdate.setString(n + 1, item.data.nr); // nr
                    pstmtUpdate.setString(++n + 1, item.data.navn); // navn
                    pstmtUpdate.setBoolean(++n + 1, Boolean.valueOf(item.data.stormodtager)); // stormodtager
                    pstmtUpdate.setString(++n + 1, item.data.nr); // nr
                    pstmtUpdate.executeUpdate();
                    cUpdate++;
                    break;
                case "delete":
                    pstmtDelete.setString(1, item.data.nr); // nr
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
    final class PostnumreObj {
        /**
         *
         */
        class DataObj {
            String nr;
            String navn;
            String stormodtager;
        }

        String operation;
        String tidspunkt;
        Integer sekvensnummer;
        DataObj data;

        /**
         * @param operation
         * @param tidspunkt
         * @param sekvensnummer
         * @param data
         */
        public PostnumreObj(String operation, String tidspunkt, Integer sekvensnummer, DataObj data) {
            this.operation = operation;
            this.tidspunkt = tidspunkt;
            this.sekvensnummer = sekvensnummer;
            this.data = data;
        }
    }
}