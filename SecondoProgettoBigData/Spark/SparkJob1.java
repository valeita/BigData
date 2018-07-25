package spark.tweet_spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import scala.Function;



public class SparkJob1 {

  public static void main(final String[] args) throws InterruptedException {
	  
    SparkSession spark = SparkSession.builder()
	    .master("local")
	    .appName("MongoSparkConnectorIntro")
	    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/clusterdb.tweets")
	    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/clusterdb.sparkBatchResultJob1")
	    .getOrCreate();

    // Create a JavaSparkContext using the SparkSession's SparkContext object
	JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	
	// Load data and infer schema, disregard toDF() name as it returns Dataset
	Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
	implicitDS.printSchema();
	implicitDS.show();

	
	// Create the temp view and execute the query
	implicitDS.createOrReplaceTempView("tweets");
	Dataset<Row> resultJob1 = spark.sql("SELECT text, retweet_count FROM tweets ORDER BY retweet_count DESC LIMIT 10");
	resultJob1.show();
	
	//Write the data to the "hundredClub" collection
	MongoSpark.write(resultJob1)
	.option("collection", "sparkBatchResultJob1")
	//.mode("append")
	.mode("overwrite")
	.save();
	
	jsc.close();	  
  }
}

