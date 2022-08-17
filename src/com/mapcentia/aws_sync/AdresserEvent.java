package com.mapcentia.aws_sync;

import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

/**
 * Created by mh on 03/02/17.
 */
final class AdresserEvent extends Stream {

    /**
     * @param sekvensNummerFra
     * @param sekvensNummerTil
     * @param c
     * @return
     * @throws Exception
     */
    AdresserObj[] get(int sekvensNummerFra, int sekvensNummerTil, Connection c) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "adresser";

        String url = "https://dawa.aws.dk/replikering/adresser/haendelser?sekvensnummerfra=" + (sekvensNummerFra + 1) + "&sekvensnummertil=" + sekvensNummerTil;
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
        AdresserEvent.AdresserObj[] Adresser = g.fromJson(response.toString(), AdresserEvent.AdresserObj[].class);

        // Prepare statements
        // ==================

        PreparedStatement pstmtDelete = c.prepareStatement("DELETE FROM " + rel + " WHERE id=?");
        PreparedStatement pstmtInsert = c.prepareStatement("INSERT INTO " + rel + " (id,status,oprettet,aendret,ikrafttraedelsesdato,adgangsadresseid,etage,doer,kilde,esdhreference,journalnummer) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtUpdate = c.prepareStatement("UPDATE " + rel + " SET status=?, oprettet=?, aendret=?," +
                "ikrafttraedelsesdato=?, adgangsadresseid=?, etage=?, doer=?, kilde=?, esdhreference=?, journalnummer=?  WHERE id=?");

        // Execute
        // =======

        for (AdresserEvent.AdresserObj item : Adresser) {
            int n = 0;
            switch (item.operation) {
                case "insert":
                    //System.out.println(item.operation);
                    pstmtInsert.setObject(n + 1, UUID.fromString(item.data.id)); // id
                    pstmtInsert.setInt(++n + 1, Integer.valueOf(item.data.status)); // status
                    pstmtInsert.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.oprettet.replace("T", " ").replace("Z", ""))); // oprettet
                    pstmtInsert.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.ændret.replace("T", " ").replace("Z", ""))); // aendret
                    pstmtInsert.setTimestamp(++n + 1, (item.data.ikrafttrædelsesdato != null) ? java.sql.Timestamp.valueOf(item.data.ikrafttrædelsesdato.replace("T", " ").replace("Z", "")) : null); // ikrafttraedelsesdato
                    pstmtInsert.setObject(++n + 1, (item.data.adgangsadresseid != null) ? UUID.fromString(item.data.adgangsadresseid) : null); // adgangsadresseid
                    pstmtInsert.setString(++n + 1, (item.data.etage != null) ? item.data.etage : null); // etage
                    pstmtInsert.setString(++n + 1, (item.data.dør != null) ? item.data.dør : null); // doer
                    pstmtInsert.setString(++n + 1, (item.data.kilde != null) ? item.data.kilde : null); // kilde
                    pstmtInsert.setString(++n + 1, (item.data.esdhreference != null) ? item.data.esdhreference : null); // esdhreference
                    pstmtInsert.setString(++n + 1, (item.data.journalnummer != null) ? item.data.journalnummer : null); // journalnummer
                    pstmtInsert.executeUpdate();
                    cInsert++;
                    break;
                case "update":
                    //System.out.println(item.operation);
                    pstmtUpdate.setInt(n + 1, Integer.valueOf(item.data.status)); // status
                    pstmtUpdate.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.oprettet.replace("T", " ").replace("Z", ""))); // oprettet
                    pstmtUpdate.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.ændret.replace("T", " ").replace("Z", ""))); // aendret
                    pstmtUpdate.setTimestamp(++n + 1, (item.data.ikrafttrædelsesdato != null) ? java.sql.Timestamp.valueOf(item.data.ikrafttrædelsesdato.replace("T", " ").replace("Z", "")) : null); // ikrafttraedelsesdato
                    pstmtUpdate.setObject(++n + 1, (item.data.adgangsadresseid != null) ? UUID.fromString(item.data.adgangsadresseid) : null); // adgangsadresseid
                    pstmtUpdate.setString(++n + 1, (item.data.etage != null) ? item.data.etage : null); // etage
                    pstmtUpdate.setString(++n + 1, (item.data.dør != null) ? item.data.dør : null); // doer
                    pstmtUpdate.setString(++n + 1, (item.data.kilde != null) ? item.data.kilde : null); // kilde
                    pstmtUpdate.setString(++n + 1, (item.data.esdhreference != null) ? item.data.esdhreference : null); // esdhreference
                    pstmtUpdate.setString(++n + 1, (item.data.journalnummer != null) ? item.data.journalnummer : null); // journalnummer
                    pstmtUpdate.setObject(++n + 1, UUID.fromString(item.data.id)); // id
                    cUpdate++;
                    pstmtUpdate.executeUpdate();
                    break;
                case "delete":
                    //System.out.println(item.data.id);
                    pstmtDelete.setObject(1, UUID.fromString(item.data.id)); // id
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
    final class AdresserObj {
        /**
         *
         */
        class DataObj {
            String id;
            int status;
            String oprettet;
            String ændret;
            String ikrafttrædelsesdato;
            String etage;
            String dør;
            String adgangsadresseid;
            String kilde;
            String esdhreference;
            String journalnummer;

        }

        String operation;
        String tidspunkt;
        Integer sekvensnummer;
        AdresserEvent.AdresserObj.DataObj data;

        /**
         *
         * @param operation
         * @param tidspunkt
         * @param sekvensnummer
         * @param data
         */
        public AdresserObj(String operation, String tidspunkt, Integer sekvensnummer, AdresserEvent.AdresserObj.DataObj data) {
            this.operation = operation;
            this.tidspunkt = tidspunkt;
            this.sekvensnummer = sekvensnummer;
            this.data = data;
        }
    }
}
