package com.mapcentia.aws_sync;

import com.google.gson.Gson;
import org.postgis.PGgeometry;
import org.postgis.Point;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

/**
 * Created by mh on 03/02/17.
 */
final class AdgangsAdresserEvent extends Stream {

    /**
     *
     * @param sekvensNummerFra
     * @param sekvensNummerTil
     * @param c
     * @return
     * @throws Exception
     */
    AdgangsAdresserObj[] get(int sekvensNummerFra, int sekvensNummerTil, Connection c) throws Exception {

        Configuration configuration = new Configuration();
        String rel = configuration.getSchema() + "." + "adgangsadresser";

        String url = "http://dawa.aws.dk/replikering/AdgangsAdresser/haendelser?sekvensnummerfra=" + (sekvensNummerFra +1) + "&sekvensnummertil=" + sekvensNummerTil;
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
        AdgangsAdresserObj[] AdgangsAdresser = g.fromJson(response.toString(), AdgangsAdresserEvent.AdgangsAdresserObj[].class);

        // Prepare statements
        // ==================

        PreparedStatement pstmtDelete = c.prepareStatement("DELETE FROM " + rel + " WHERE id=?");
        PreparedStatement pstmtInsert = c.prepareStatement("INSERT INTO " + rel + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement pstmtUpdate = c.prepareStatement("UPDATE " + rel + " SET status=?, kommunekode=?, vejkode=?, husnr=?, supplerendebynavn=?, postnr=?, oprettet=?, aendret=?," +
                "ikrafttraedelsesdato=?,  ejerlavkode=?, matrikelnr=?, esrejendomsnr=?, etrs89koordinat_oest=?, etrs89koordinat_nord=?, hoejde=?, noejagtighed=?, kilde=?, husnummerkilde=?, tekniskstandard=?," +
                "tekstretning=?, esdhreference=?, journalnummer=?, adressepunktaendringsdato=?, the_geom=? WHERE id=?");

        // Execute
        // =======

        for (AdgangsAdresserObj item : AdgangsAdresser) {
            int n = 0;
            switch (item.operation) {
                case "insert":
                    //System.out.println(item.operation);
                    pstmtInsert.setObject(n + 1, UUID.fromString(item.data.id)); // id
                    pstmtInsert.setInt(++n + 1, item.data.status); // status
                    pstmtInsert.setString(++n + 1, (item.data.kommunekode != null) ? item.data.kommunekode : null); // kommunekode
                    pstmtInsert.setString(++n + 1, (item.data.vejkode != null) ? item.data.vejkode : null); // vejkode
                    pstmtInsert.setString(++n + 1, (item.data.husnr != null) ? item.data.husnr : null); // husnr
                    pstmtInsert.setString(++n + 1, (item.data.supplerendebynavn != null) ? item.data.supplerendebynavn : null); // supplerendebynavn
                    pstmtInsert.setString(++n + 1, (item.data.postnr != null) ? item.data.postnr : null); // postnr
                    pstmtInsert.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.oprettet.replace("T", " ").replace("Z", ""))); // oprettet
                    pstmtInsert.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.ændret.replace("T", " ").replace("Z", ""))); // aendret
                    pstmtInsert.setTimestamp(++n + 1, (item.data.ikrafttrædelsesdato != null) ? java.sql.Timestamp.valueOf(item.data.ikrafttrædelsesdato.replace("T", " ").replace("Z", "")) : null); // ikrafttraedelsesdato
                    pstmtInsert.setString(++n + 1, (item.data.ejerlavkode != null) ? item.data.ejerlavkode : null); // ejerlavkode
                    pstmtInsert.setString(++n + 1, (item.data.matrikelnr != null) ? item.data.matrikelnr : null); // matrikelnr
                    pstmtInsert.setString(++n + 1, (item.data.esrejendomsnr != null) ? item.data.esrejendomsnr : null); // esrejendomsnr
                    pstmtInsert.setFloat(++n + 1, (item.data.etrs89koordinat_øst != null) ? Float.valueOf(item.data.etrs89koordinat_øst) : 0); // etrs89koordinat_oest
                    pstmtInsert.setFloat(++n + 1, (item.data.etrs89koordinat_nord != null) ? Float.valueOf(item.data.etrs89koordinat_nord) : 0); // etrs89koordinat_nord
                    pstmtInsert.setFloat(++n + 1, (item.data.højde != null) ? Float.valueOf(item.data.højde) : 0); // hoejde
                    pstmtInsert.setString(++n + 1, (item.data.nøjagtighed != null) ? item.data.nøjagtighed : null); // noejagtighed
                    pstmtInsert.setString(++n + 1, (item.data.kilde != null) ? item.data.kilde : null); // kilde
                    pstmtInsert.setString(++n + 1, (item.data.husnummerkilde != null) ? item.data.husnummerkilde : null); // husnummerkilde
                    // Start to get out of bound
                    pstmtInsert.setString(++n + 1, (item.data.tekniskstandard != null) ? item.data.tekniskstandard : null); // tekniskstandard
                    pstmtInsert.setString(++n + 1, (item.data.tekstretning != null) ? item.data.tekstretning : null); // tekstretning
                    pstmtInsert.setString(++n + 1, (item.data.esdhreference != null) ? item.data.esdhreference : null); // esdhreference
                    pstmtInsert.setString(++n + 1, (item.data.journalnummer != null) ? item.data.journalnummer : null); // journalnummer
                    pstmtInsert.setTimestamp(++n + 1, (item.data.adressepunktændringsdato != null) ? java.sql.Timestamp.valueOf(item.data.adressepunktændringsdato.replace("T", " ").replace("Z", "")) : null); // adressepunktaendringsdato
                    // Create geometry
                    if (item.data.etrs89koordinat_øst != null && item.data.etrs89koordinat_nord != null) {
                        Point point = new Point();
                        point.setX(Float.valueOf(item.data.etrs89koordinat_øst));
                        point.setY(Float.valueOf(item.data.etrs89koordinat_nord));
                        point.setSrid(25832);
                        PGgeometry geom = new PGgeometry(point);
                        pstmtInsert.setObject(++n + 1, geom); // the_geom
                    } else {
                        pstmtInsert.setObject(++n + 1, null); // the_geom
                    }
                    pstmtInsert.executeUpdate();
                    cInsert++;
                    break;
                case "update":
                    //System.out.println(item.operation);
                    pstmtUpdate.setInt(n + 1, item.data.status); // status
                    pstmtUpdate.setString(++n + 1, (item.data.kommunekode != null) ? item.data.kommunekode : null); // kommunekode
                    pstmtUpdate.setString(++n + 1, (item.data.vejkode != null) ? item.data.vejkode : null); // vejkode
                    pstmtUpdate.setString(++n + 1, (item.data.husnr != null) ? item.data.husnr : null); // husnr
                    pstmtUpdate.setString(++n + 1, (item.data.supplerendebynavn != null) ? item.data.supplerendebynavn : null); // supplerendebynavn
                    pstmtUpdate.setString(++n + 1, (item.data.postnr != null) ? item.data.postnr : null); // postnr
                    pstmtUpdate.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.oprettet.replace("T", " ").replace("Z", ""))); // oprettet
                    pstmtUpdate.setTimestamp(++n + 1, java.sql.Timestamp.valueOf(item.data.ændret.replace("T", " ").replace("Z", ""))); // aendret
                    pstmtUpdate.setTimestamp(++n + 1, (item.data.ikrafttrædelsesdato != null) ? java.sql.Timestamp.valueOf(item.data.ikrafttrædelsesdato.replace("T", " ").replace("Z", "")) : null); // ikrafttraedelsesdato
                    pstmtUpdate.setString(++n + 1, (item.data.ejerlavkode != null) ? item.data.ejerlavkode : null); // ejerlavkode
                    pstmtUpdate.setString(++n + 1, (item.data.matrikelnr != null) ? item.data.matrikelnr : null); // matrikelnr
                    pstmtUpdate.setString(++n + 1, (item.data.esrejendomsnr != null) ? item.data.esrejendomsnr : null); // esrejendomsnr
                    pstmtUpdate.setFloat(++n + 1, (item.data.etrs89koordinat_øst != null) ? Float.valueOf(item.data.etrs89koordinat_øst) : 0); // etrs89koordinat_oest
                    pstmtUpdate.setFloat(++n + 1, (item.data.etrs89koordinat_nord != null) ? Float.valueOf(item.data.etrs89koordinat_nord) : 0); // etrs89koordinat_nord
                    pstmtUpdate.setFloat(++n + 1, (item.data.højde != null) ? Float.valueOf(item.data.højde) : 0); // hoejde
                    pstmtUpdate.setString(++n + 1, (item.data.nøjagtighed != null) ? item.data.nøjagtighed : null); // noejagtighed
                    pstmtUpdate.setString(++n + 1, (item.data.kilde != null) ? item.data.kilde : null); // kilde
                    pstmtUpdate.setString(++n + 1, (item.data.husnummerkilde != null) ? item.data.husnummerkilde : null); // husnummerkilde
                    // Start to get out of bound
                    pstmtUpdate.setString(++n + 1, (item.data.tekniskstandard != null) ? item.data.tekniskstandard : null); // tekniskstandard
                    pstmtUpdate.setString(++n + 1, (item.data.tekstretning != null) ? item.data.tekstretning : null); // tekstretning
                    pstmtUpdate.setString(++n + 1, (item.data.esdhreference != null) ? item.data.esdhreference : null); // esdhreference
                    pstmtUpdate.setString(++n + 1, (item.data.journalnummer != null) ? item.data.journalnummer : null); // journalnummer
                    pstmtUpdate.setTimestamp(++n + 1, (item.data.adressepunktændringsdato != null) ? java.sql.Timestamp.valueOf(item.data.adressepunktændringsdato.replace("T", " ").replace("Z", "")) : null); // adressepunktaendringsdato
                    // Create geometry
                    if (item.data.etrs89koordinat_øst != null && item.data.etrs89koordinat_nord != null) {
                        Point point = new Point();
                        point.setX(Float.valueOf(item.data.etrs89koordinat_øst));
                        point.setY(Float.valueOf(item.data.etrs89koordinat_nord));
                        point.setSrid(25832);
                        PGgeometry geom = new PGgeometry(point);
                        pstmtUpdate.setObject(++n + 1, geom); // the_geom
                    } else {
                        pstmtUpdate.setObject(++n + 1, null); // the_geom
                    }
                    pstmtUpdate.setString(++n + 1, item.data.id); // id
                    pstmtUpdate.executeUpdate();
                    cUpdate++;
                    break;
                case "delete":
                    //System.out.println(item.data.id);
                    pstmtDelete.setObject(1, item.data.id); // id
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
        return AdgangsAdresser;
    }

    /**
     *
     */
    final class AdgangsAdresserObj {
        /**
         *
         */
        class DataObj {
            String id;
            int status;
            String kommunekode;
            String vejkode;
            String husnr;
            String supplerendebynavn;
            String postnr;
            String oprettet;
            String ændret;
            String ikrafttrædelsesdato;
            String ejerlavkode;
            String matrikelnr;
            String esrejendomsnr;
            String etrs89koordinat_øst;
            String etrs89koordinat_nord;
            String højde;
            String nøjagtighed;
            String kilde;
            String husnummerkilde;
            String tekniskstandard;
            String tekstretning;
            String esdhreference;
            String journalnummer;
            String adressepunktændringsdato;
        }
        String operation;
        String tidspunkt;
        Integer sekvensnummer;
        AdgangsAdresserEvent.AdgangsAdresserObj.DataObj data;

        /**
         *
         * @param operation
         * @param tidspunkt
         * @param sekvensnummer
         * @param data
         */
        public AdgangsAdresserObj(String operation, String tidspunkt, Integer sekvensnummer, AdgangsAdresserEvent.AdgangsAdresserObj.DataObj data) {
            this.operation = operation;
            this.tidspunkt = tidspunkt;
            this.sekvensnummer = sekvensnummer;
            this.data = data;
        }
    }
}
