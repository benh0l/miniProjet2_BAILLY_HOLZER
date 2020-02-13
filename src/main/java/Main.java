import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
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
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        JavaRDD<Row> verRow = sc.parallelize(Arrays.asList(RowFactory.create(1,"A"),RowFactory.create(2,"B")));
        JavaRDD<Row> edgRow = sc.parallelize(Arrays.asList(RowFactory.create(1,2,"Edge")));

        List<StructField> verFields = new ArrayList<StructField>();
        verFields.add(DataTypes.createStructField("id",DataTypes.IntegerType, true));
        verFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));

        List<StructField> EdgFields = new ArrayList<StructField>();
        EdgFields.add(DataTypes.createStructField("fromId",DataTypes.IntegerType, true));
        EdgFields.add(DataTypes.createStructField("toId",DataTypes.IntegerType, true));
        EdgFields.add(DataTypes.createStructField("name",DataTypes.StringType, true));

        StructType verSchema = DataTypes.createStructType(verFields);
        StructType edgSchema = DataTypes.createStructType(EdgFields);

        Dataset<Row> verDF = sqlContext.createDataFrame(verRow, verSchema);
        Dataset<Row> edgDF = sqlContext.createDataFrame(edgRow, edgSchema);

        GraphFrame g = new GraphFrame(verDF,edgDF);
        g.vertices().show();
        g.edges().show();
        g.persist(StorageLevel.MEMORY_AND_DISK());

        //Question 5
        Dataset<Row> degrees = g.degrees();
        degrees.show(false);
    }

}