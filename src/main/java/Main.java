import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.graphframes.lib.TriangleCount;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Main {

    public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSessionExample")
                .config("spark.sql.warehouse.dir", "/file:C:/temp")
                .config("spark.sql.crossJoin.enabled", true)
                .getOrCreate();
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        StructType verSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");

        Dataset<Row> verFields = spark.read().option("mode", "DROPMALFORMED").schema(verSchema).csv("src/main/resources/airports.dat");

        StructType edgSchema = new StructType().add("airline", "string").add("airlineId", "int").add("sourceAirport", "string").add("src", "int").add("destinationAirport", "string").add("dst", "int")
                .add("codeShare", "string").add("stops", "int").add("equipment", "string");

        Dataset<Row> edgFields = spark.read().option("mode", "DROPMALFORMED").schema(edgSchema).csv("src/main/resources/routes.dat");

        System.out.println("-----------------QUESTION 3 & 4-----------------");
        GraphFrame g = new GraphFrame(verFields, edgFields);

        System.out.println("----------------VERTICES----------------");
        g.vertices().show();

        System.out.println("----------------EDGES----------------");
        g.edges().show();
        g.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("----------------QUESTION 5----------------");
        //Question 5
        Dataset<Row> degrees = g.degrees();
        degrees.show(false);

        System.out.println("----------------QUESTION 6----------------");
        //Question 6
        Dataset<Row> indegrees = g.inDegrees();
        indegrees.show(false);

        System.out.println("----------------QUESTION 7----------------");
        //Question 7
        Dataset<Row> outdegrees = g.outDegrees();
        outdegrees.show(false);

        System.out.println("----------------QUESTION 8----------------");
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

        System.out.println("----------------QUESTION 9----------------");
        //Question 9
        /*
        Dataset<Row> triplet = g.triangleCount().run();
        triplet.orderBy(
                functions.col("count").desc()
        ).show(false);
        */


        System.out.println("----------------QUESTION 10----------------");
        //Question 10
        System.out.println("Airports count: "+g.vertices().count());
        System.out.println("Trips count: "+g.edges().count());



        System.out.println("----------------QUESTION 11_1----------------");
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

        System.out.println("----------------QUESTION 11_2----------------");
        //Question 11_2

        Dataset<Row> delay3 = delay.select("ORIGIN", "DEST","DEP_DELAY")
                .filter("ORIGIN = 'SEA'")
                .groupBy("DEST")
                .avg("DEP_DELAY")
                .filter("avg(DEP_DELAY) > 10")
                .orderBy(functions.col("avg(DEP_DELAY)").desc());
        delay3.show(false);

        System.out.println("----------------QUESTION 12_1----------------");
        //Question 12_1

        Dataset<Row> delay4 = delay.filter("ORIGIN = 'SFO'")
                .filter("DEST = 'JAC'")
                .join(
                        delay.filter("ORIGIN = 'JAC'")
                                .filter("DEST = 'SEA'")
                );
        delay4.show(false);

        System.out.println("----------------QUESTION 12_2----------------");
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

        System.out.println("----------------QUESTION 12_3----------------");
        //Question 12_3

        Dataset<Row> delay6 = delay.select("ORIGIN", "DEST")
                .distinct()
                .filter("ORIGIN = 'SFO'");
        delay6.show(false);
    }

}
