package PrimoProgetto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import scala.Tuple3;
import utils.*;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.Iterator;

public class Job2Spark {

  private String pathToFile;
  
  public Job2Spark(String file){
	  this.pathToFile = file;
  }
  
  public static void main(String[] args) {
		
		if (args.length < 1) {
	        System.err.println("ERROR: no such input");
	        System.exit(1);
	      }
		
		Job2Spark wc = new Job2Spark(args[0]);
		
		JavaPairRDD<String, Iterable<Tuple3<String, Integer, Double>>> jprdd= wc.job2Red();
		
		DecimalFormat f = new DecimalFormat("##.00");
		for(Tuple2<String, Iterable<Tuple3<String, Integer, Double>>> record : jprdd.collect()) {
			Iterator<Tuple3<String, Integer, Double>> it = record._2().iterator();
			System.out.print(record._1()+"-> ");
			for(int i = 0; i<10; i++) 
				
				{
					if(!it.hasNext()) break;
					Tuple3<String,Integer,Double> tmp= it.next();
					System.out.print(" "+tmp._2()+":"+f.format(tmp._3())+"; ");
					
				}
			System.out.println();
		}
	
  }

  public JavaPairRDD<Tuple2<String, Integer>,Integer> loadData() {
  
    SparkConf conf = new SparkConf()
        .setAppName("Job2")
        .setMaster("local[*]"); 
    @SuppressWarnings("resource")
	JavaSparkContext sc = new JavaSparkContext(conf);
    JavaPairRDD<Tuple2<String, Integer>,Integer> yearsPS = sc.textFile(pathToFile)
			 												 .mapToPair(line -> new Tuple2<Tuple2<String, Integer>,Integer>(ParserJob2.parseReview(line)._2(),
			 														  ParserJob2.parseReview(line)._1()))
			 												 .filter(yearPS ->(yearPS._1._2<=2012 && yearPS._1._2>2002));

    return yearsPS;

  }

  
  public JavaPairRDD<String, Iterable<Tuple3<String, Integer, Double>>> job2Red() {
	
	JavaPairRDD<Tuple2<String, Integer>,Integer>rdd1 = loadData();
    JavaRDD<Tuple3<String,Integer,Double>> rdd2= rdd1.aggregateByKey(new StatCounter(), StatCounter::merge, StatCounter::merge)
    														.sortByKey(new TupleMapIntegerComparator(), true, 1)
    														.map(line -> new Tuple3<String,Integer,Double>(line._1._1,line._1._2,line._2.mean()));
    
    JavaPairRDD<String, Iterable<Tuple3<String, Integer, Double>>> rdd3 = rdd2.groupBy(x-> x._1(),1).sortByKey();
    
    return rdd3;
  }
  private static class TupleMapIntegerComparator implements Comparator<Tuple2<String,Integer>>, Serializable {
	    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

		@Override
	    public int compare(Tuple2<String,Integer> tuple1, Tuple2<String,Integer> tuple2) {

	        if (tuple1._2.compareTo(tuple2._2) == 0) {
	            return tuple1._1.compareTo(tuple2._1);
	        }
	        return tuple1._2.compareTo(tuple2._2);
	    }
	}



}
