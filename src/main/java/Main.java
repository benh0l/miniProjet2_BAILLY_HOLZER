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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.graphframes.GraphFrame;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class Main {

    public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException{

        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSessionExample")
                .getOrCreate();
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        StructType verSchema = new StructType().add("id", "int").add("Name", "string").add("City", "string").add("Country", "string").add("IATA", "string").add("ICAO", "string")
                .add("Latitude", "double").add("Longitude", "double").add("Altitude", "int").add("Timezone", "int")
                .add("DST", "string").add("Tz database time zone", "string").add("Type", "string").add("Source", "string");

        Dataset<Row> verFields= spark.read().option("mode", "DROPMALFORMED").schema(verSchema).csv("src/main/resources/airports.dat");

        StructType edgSchema = new StructType().add("airline", "string").add("airlineId", "int").add("sourceAirport", "string").add("src", "int").add("destinationAirport", "string").add("dst", "int")
                .add("codeShare", "string").add("stops", "int").add("equipment", "string");

        Dataset<Row> edgFields = spark.read().option("mode", "DROPMALFORMED").schema(edgSchema).csv("src/main/resources/routes.dat");

        System.out.println("-----------------QUESTION 3 & 4-----------------");
        GraphFrame g = new GraphFrame(verFields,edgFields);

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

    }

}
