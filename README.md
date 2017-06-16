#Repliker hele Danmarks Adresseregister
Repliker hele Danmarks Adresseregister, DAR, til lokal database og hold den opdateret i nær realtid.

##Hvordan bruges replikatoren
Replikatoren er et kommandolinje-værktøj skrevet i Java og derfor kan anvendes på alle platforme (Windows, Linux, Mac mv.)
 
Pt. er det kun PostgreSQL databaseserveren, som er understøttet.

###Forudsætninger
- Et Java 8 Runtime
- PostgreSQL med PostGIS

###Installering
- Hent zip filen på: https://github.com/mapcentia/DAWAreplicator/archive/master.zip
- Udpak den
- Kopiere DAWAreplicator-master/bin/DAWAreplicator.jar til hvor du vil have den
- opret en konfigurationsfil (kan lægges i samme folder som jar-filen ), kald den fx config.yml kopiere dette til den og ret til:



```
#PostgreSQL forbindelse. Databasen "min_dar_database" skal have Postgis extension aktiveret   

connection:
      url: jdbc:postgresql://127.0.0.1:5432/min_dar_database
      user: postgres
      pw: 1234

# Database schema hvor data havner. Scahemaet skal være oprettet   

schema: dar
```

- Kør replikatoren første gang (med "init" argumentet). Hele DAR bliver kopieret til databasen:
```
java -jar DAWAreplicator.jar config.yml init
```

Efterfølgende kan databasen opdateres med ændringer ved at køre kommandoen uden "init":
```
java -jar DAWAreplicator.jar config.yml
```

Ovenstående kommando kan indsættes i en job scheduler og køres fx hvert 10. minut. 