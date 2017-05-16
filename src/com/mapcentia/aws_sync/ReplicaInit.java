package com.mapcentia.aws_sync;

/**
 *
 */
final class ReplicaInit {
    /**
     * @throws InterruptedException
     */
    void start() throws Exception {

        // Get sekvens
        // ===========

        Sekvens.SekvensObj res;
        Sekvens sekvens = new Sekvens();
        res = sekvens.get();

        // Store the sekvens
        // =================

        sekvens.storeInDb(res.sekvensnummer);

        if (res != null) {
            System.out.print("sekvensnummer: " + res.sekvensnummer);
            System.out.println("   Tid: " + res.tidspunkt);
            System.out.println();
        }

/*        // Get postnumre
        PostnumreInit postnumre = new PostnumreInit();
        postnumre.get(res.sekvensnummer);

        System.out.println();

        // Get elerlav
        EjerlavInit ejerlav = new EjerlavInit();
        ejerlav.get(res.sekvensnummer);

        System.out.println();

        // Get vejstykker
        VejstykkerInit vejstykker = new VejstykkerInit();
        vejstykker.get(res.sekvensnummer);

        System.out.println();

        // Get adresser
        AdresserInit adresser = new AdresserInit();
        adresser.get(res.sekvensnummer);

        System.out.println();*/

        // Get adgangsadresser
        AdgangsAdresserInit adgangsAdresser = new AdgangsAdresserInit();
        adgangsAdresser.get(res.sekvensnummer);

        System.out.println();
    }
}
