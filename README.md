# Rapport mini-projet-2 BigData

## Julien Bailly - Benoît Holzer

###Utilisation :

Il suffit d'exécuter la classe principale Main.java sans arguments.

---
###Questions 1 à 4

```java
StructType verSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
       .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
       .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");

Dataset<Row> verFields = spark.read().option("mode", "DROPMALFORMED").schema(verSchema).csv("src/main/resources/airports.dat");

StructType edgSchema = new StructType().add("airline", "string").add("airlineId", "int").add("sourceAirport", "string").add("src", "int").add("destinationAirport", "string").add("dst", "int")
       .add("codeShare", "string").add("stops", "int").add("equipment", "string");

Dataset<Row> edgFields = spark.read().option("mode", "DROPMALFORMED").schema(edgSchema).csv("src/main/resources/routes.dat");
```

Le code ci-dessus correspond à la création des vertices et des edges contenu dans le fichier airports.dat. Le fichier ayant pour extension .dat et non .csv il a fallu définir le schéma des données à l’aide d’un objet StructType puis utiliser les méthodes schema() et csv() pour obtenir le même résultat qu’avec un fichier .csv.

```java
System.out.println("-----------------QUESTION 3 & 4-----------------");
GraphFrame g = new GraphFrame(verFields, edgFields);

System.out.println("----------------VERTICES----------------");
g.vertices().show();

System.out.println("----------------EDGES----------------");
g.edges().show();
g.persist(StorageLevel.MEMORY_AND_DISK());
̀̀```
Le code ci-dessus permet l’affichage des edges et des vertices. g.persist(StorageLevel.MEMORY_AND_DISK()) permet d’enregistrer la RDD dans la mémoire et si la place n’est pas suffisante de l’enregistrer sur le disque.

---
### Questions 5 à 7
```java
System.out.println("----------------QUESTION 5----------------");
//Question 5
Dataset<Row> degrees = g.degrees();
degrees.sort().show(false);

System.out.println("----------------QUESTION 6----------------");
//Question 6
Dataset<Row> indegrees = g.inDegrees();
indegrees.sort().show(false);

System.out.println("----------------QUESTION 7----------------");
//Question 7
Dataset<Row> outdegrees = g.outDegrees();
outdegrees.sort().show(false);
```
On récupère les informations qui nous intéresse grâce aux méthodes liées (degrees(), indegrees() et outdegrees()) dans un Dataset puis on les affiche simplement à l’aide de la méthode show().

---
### Question 8

```java
//Question 8
Dataset<Row> inout = indegrees.join(outdegrees, "id")
       .select(
               functions.col("id"),
               functions.col("indegree").divide(functions.col("outdegree")).as("transfertsRatio")
       )
       .orderBy(
               functions.abs(functions.col("transfertsRatio").minus(1))
       );
inout.show(false);
```

L’objectif ici est d’obtenir les aéroports avec une valeur inDegrees/outDegrees la plus proche possible de 1. Pour cela on réalise l’opération inDegrees/outDegrees - 1 pour obtenir des valeurs autour de 0 et non plus de 1 puis on passe en valeur absolue tous les résultats. Ainsi on a juste a récupéré nos résultats dans l’ordre croissant pour obtenir ce que l’on souhaitait au départ.
On affiche ensuite notre Dataset obtenu.

---
### Question 9

```java
Dataset<Row> triplet = g.triangleCount().run();
triplet.orderBy(
       functions.col("count").desc()
).show(false);
```
Avec spark il est facile de répondre à cette question grâce à la méthode triangleCount(), on stocke notre résultat dans un dataset puis on trie par la colonne “count” par ordre décroissant et on affiche notre résultat.

---
### Question 10

```java
System.out.println("Airports count: "+g.vertices().count());
System.out.println("Trips count: "+g.edges().count());
```

---
###Question 11_1
Pour les questions 11 et 12 nous avons utilisé un grapheSet sur le fichier csv avec les delay et nous avons fait des requêtes dedans.

```java
//Question 11_1

StructType delaySchema = new StructType()
       .add("ORIGIN_AIRPORT_ID", "string")
       .add("ORIGIN", "string")
       .add("DEST_AIRPORT_ID", "string")
       .add("DEST", "string")
       .add("DEP_DELAY", "double")
       .add("DEP_DELAY_NEW", "double")
       .add("DEP_DEL15", "double");

Dataset<Row> delay = spark.read().option("header", true).schema(delaySchema).csv("src/main/resources/q11-12.csv");

Dataset<Row> delay2 = delay.filter("ORIGIN == 'SFO'").orderBy(
       functions.col("DEP_DELAY").desc()
);
delay2.show(false);
```

Le header du fichier csv ne permettait pas à l’application de définir le type des données c’est pour cette raison que nous avons définis nous même le schéma pour pouvoir par la suite comparer et additionner les delay en tant que double et non des string.

Ici nous filtrons les vols qui ont pour départ SFO et nous trions sur le delay par ordre décroissant pour avoir les plus gros delay en premier.

---
###Question 11_2

```java
//Question 11_2

Dataset<Row> delay3 = delay.select("ORIGIN", "DEST","DEP_DELAY")
       .filter("ORIGIN = 'SEA'")
       .groupBy("DEST")
       .avg("DEP_DELAY")
       .filter("avg(DEP_DELAY) > 10")
       .orderBy(functions.col("avg(DEP_DELAY)").desc());
delay3.show(false);
```

Pour cette question nous affichons toute combinaison SEA → Dest avec une moyenne des retards supérieurs à 10.
Pour cela il faut groupBy dest pour pouvoir faire la moyenne sur le delay. Ensuite nous filtrons sur les moyennes de delay supérieurs à 10.

Format du  résultat de la requête (ce résultat n’est pas celui obtenu c’est un exemple du format du résultat):

ORIGIN	DEST		avg(DELAY)
SEA		JAC		19.6
SEA		SFO		15.4


---
###Question 12_1

```java
//Question 12_1

Dataset<Row> delay4 = delay.filter("ORIGIN = 'SFO'")
       .filter("DEST = 'JAC'")
       .join(
               delay.filter("ORIGIN = 'JAC'")
                       .filter("DEST = 'SEA'")
       );
delay4.show(false);
```

Pour la question 12_1 nous affichons la jointure des vols qui partent de SFO vers JAC et les vols qui partent de JAC à SEA grâce un un join et à des filter sur la destination et les origins.

---
###Question 12_2

```java
//Question 12_2

Dataset<Row> delay5 = delay.select(functions.col("ORIGIN_AIRPORT_ID"),functions.col("ORIGIN"), functions.col("DEST_AIRPORT_ID"), functions.col("DEST"), functions.col("DEP_DELAY").as("delay1"))
       .filter("ORIGIN = 'SFO'")
       .filter("DEST = 'JAC'")
       .join(
               delay.select(functions.col("ORIGIN_AIRPORT_ID"),functions.col("ORIGIN"), functions.col("DEST_AIRPORT_ID"), functions.col("DEST"), functions.col("DEP_DELAY").as("delay2"))
                       .filter("ORIGIN = 'JAC'")
                       .filter("DEST = 'SEA'")
       ).filter("delay1 + delay2 < -5");
delay5.show(false);
```

Cette requête est la même que la question d’avant avec un filter sur les delay des vols, pour avoir la somme des delay des vols SFO → JAC et JAC → SEA nous avons du différencier les 2 colonnes delay pour pouvoir les additionner ensuite dans le filter. Cela est possible grâce à la méthode as().

---
###Question 12_3

```java
//Question 12_3

Dataset<Row> delay6 = delay.select("ORIGIN", "DEST")
       .distinct()
       .filter("ORIGIN = 'SFO'");
delay6.show(false);
```

Pour cette dernière requête nous avons pas très bien compris la question alors nous avons simplement affiché les vols avec pour origines SFO.
