package PrimoProgetto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import utils.ParserJob1;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;



public class Job1Spark {

  private String pathToFile;
  
  public Job1Spark(String file){
	  this.pathToFile = file;
  }
  
  public static void main(String[] args) {
		
		if (args.length < 1) {
	        System.err.println("ERROR: no such input");
	        System.exit(1);
	      }
		
		Job1Spark wc = new Job1Spark(args[0]);
		
		JavaPairRDD<Integer, Iterable<Tuple3<Integer, String, Integer>>> job1result= wc.job1();
		
		for(Tuple2<Integer, Iterable<Tuple3<Integer, String, Integer>>> record : job1result.collect()) {
			Iterator<Tuple3<Integer, String, Integer>> it = record._2().iterator();
			for(int i = 0; i<10; i++) System.out.println(it.next().toString());
		}
	
		
		
  }

  public JavaPairRDD<Tuple2<Integer,String>,Integer> loadData() {
  
    SparkConf conf = new SparkConf()
        .setAppName("Wordcount")
        .setMaster("local[*]"); 

    @SuppressWarnings("resource")
	JavaSparkContext sc = new JavaSparkContext(conf);
   
    JavaPairRDD<Tuple2<Integer, String>, Integer> yearWord = sc.textFile(pathToFile)
    											 .mapToPair(line -> new Tuple2<Integer, String>(ParserJob1.parseLineToPairYearSummary(line)._1(),
    													 										ParserJob1.parseLineToPairYearSummary(line)._2()))
    											 .flatMapValues(summary -> Arrays.asList(summary.split(" ")))
    											 .mapToPair(x -> new Tuple2<Tuple2<Integer,String>,Integer>(new Tuple2<Integer,String>(x._1,x._2),(Integer) 1));
    
   

    return yearWord;

  }

  
  public JavaPairRDD<Integer, Iterable<Tuple3<Integer, String, Integer>>> job1() {
	  
    JavaPairRDD<Tuple2<Integer, String>, Integer> yearWord = loadData();

    JavaPairRDD<Tuple2<Integer,String>,Integer> yearWordAggregation =  yearWord.reduceByKey((a, b) -> a + b);
    JavaRDD<Tuple3<Integer,String,Integer>> yearWordFreq = yearWordAggregation.map(x -> 
    										new Tuple3<Integer,String,Integer>(x._1._1, x._1._2,x._2));
    
    
    JavaPairRDD<Tuple2<Integer,Integer>,String> resultOrdered =  yearWordFreq.mapToPair(x -> new Tuple2<Tuple2<Integer,Integer>,String>
    					(new Tuple2<Integer, Integer>(x._1(),x._3()),x._2())).sortByKey(new TupleMapIntegerComparator(), true, 1);

    JavaRDD<Tuple3<Integer,String,Integer>> resultOrderedTriple = resultOrdered.map(x -> 
					new Tuple3<Integer,String,Integer>(x._1._1, x._2,x._1._2));
    
    JavaPairRDD<Integer, Iterable<Tuple3<Integer, String, Integer>>> finalResult = resultOrderedTriple
    																						.groupBy(x->x._1(), 1).sortByKey()
    																						.filter(x->x._1()!=1970 && x._1()!=9999);
    
    return finalResult;
  }

  private static class TupleMapIntegerComparator implements Comparator<Tuple2<Integer,Integer>>, Serializable {
	    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

		@Override
	    public int compare(Tuple2<Integer,Integer> tuple1, Tuple2<Integer,Integer> tuple2) {

	        if (tuple1._2.compareTo(tuple2._2) == 0) {
	            return tuple1._1.compareTo(tuple2._1);
	        }
	        return -tuple1._2.compareTo(tuple2._2);
	    }
	}
  

}
  

