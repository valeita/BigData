package spark.tweet_spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import scala.Function;



public class SparkJob2 {

  public static void main(final String[] args) throws InterruptedException {
	  
	 
    SparkSession spark = SparkSession.builder()
	    .master("local")
	    .appName("MongoSparkConnectorIntro")
	    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/clusterdb.tweets")
	    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/clusterdb.sparkBatchResultJob2")
	    .getOrCreate();
    
    spark.udf().register("formatTime",
    	       (String arg1) -> {
    	    	   		return arg1.substring(0,10);
    	    	   }, DataTypes.StringType);

    // Create a JavaSparkContext using the SparkSession's SparkContext object
	JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

	// Load data and infer schema, disregard toDF() name as it returns Dataset
	
	Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
	//implicitDS.groupBy("_id").count();
	implicitDS.printSchema();
	implicitDS.show();

	
	// Create the temp view and execute the query
	implicitDS.createOrReplaceTempView("tweets");
	Dataset<Row> resultJob1 = 
			spark.sql("SELECT AVG(t.totale) as averageForDay, t.place FROM (SELECT COUNT (1) AS totale,"
					+ " formatTime(created_at) as date, place FROM tweets GROUP BY formatTime(created_at),place) t"
					+ " GROUP BY t.place ORDER BY AVG(t.totale) DESC");

	resultJob1.show();
	
	//Write the data to the "hundredClub" collection
	MongoSpark.write(resultJob1)
	.option("collection", "sparkBatchResultJob2")
	//.mode("append")
	.mode("overwrite")
	.save();
	
	jsc.close();	  
	  
	  

	  
    
  }
}

