package com.mapcentia.aws_sync;

import java.sql.Connection;

final class ReplicaEvent {

    /**
     * @throws InterruptedException
     */
    public void start() throws Exception {

        // Get sekvens
        // ===========

        Sekvens.SekvensObj res;
        Sekvens sekvens = new Sekvens();
        res = sekvens.get();
        int lastSekvens = sekvens.getLastFromDb();

        if (res != null) {
            System.out.println(res.tidspunkt);
            System.out.println("Database : " + lastSekvens);
            System.out.println("Seneste  : " + res.sekvensnummer);
            System.out.println();
        }

        // Open PG connection
        // ==================

        Connection c = Connect.open();
        c.setAutoCommit(false);

        // Get PostnumreEvent
        // ===================

        PostnumreEvent.PostnumreObj resPostnumre[];
        PostnumreEvent PostnumreEvent = new PostnumreEvent();
        resPostnumre = PostnumreEvent.get(lastSekvens, res.sekvensnummer, c);

        if (resPostnumre != null) {

        }

        // Get ejerlavEvent
        // =======================

        EjerlavEvent.EjerlavObj resEjerlav[];
        EjerlavEvent ejerlavEvent = new EjerlavEvent();
        resEjerlav = ejerlavEvent.get(lastSekvens, res.sekvensnummer, c);

        if (resEjerlav != null) {

        }

        // Get vejstykkerEvent
        // =======================

        VejstykkerEvent.VejstykkerObj resVejstykker[];
        VejstykkerEvent vejstykkerEvent = new VejstykkerEvent();
        resVejstykker = vejstykkerEvent.get(lastSekvens, res.sekvensnummer, c);

        if (resVejstykker != null) {

        }

        // Get adgangsAdresseEvent
        // =======================

        AdgangsAdresserEvent.AdgangsAdresserObj resAdgangsAdresser[];
        AdgangsAdresserEvent adgangsAdresserEvent = new AdgangsAdresserEvent();
        resAdgangsAdresser = adgangsAdresserEvent.get(lastSekvens, res.sekvensnummer, c);

        if (resAdgangsAdresser != null) {

        }

        // Get adgangsAdresseEvent
        // =======================

        AdresserEvent.AdresserObj resAdresser[];
        AdresserEvent adresserEvent = new AdresserEvent();
        resAdresser = adresserEvent.get(lastSekvens, res.sekvensnummer, c);

        if (resAdresser != null) {

        }


        // Commit statements and close connection
        // All statements are  stored or none
        // ======================================

        c.commit();
        c.close();

        // Store the sekvens
        // Only if all statements are  stored
        // ==================================

        sekvens.storeInDb(res.sekvensnummer);


        // Recursive call
        //Thread.sleep(1000);
        //start();
    }
}
