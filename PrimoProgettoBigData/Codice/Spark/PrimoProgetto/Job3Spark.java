package PrimoProgetto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utils.ParserJob3;
import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;


public class Job3Spark {

  private static String pathToFile;
  
  
  public Job3Spark(String file){
	  Job3Spark.pathToFile = file;
  }
  
  
  
  
  
  public static void main(String[] args) {
		
		if (args.length < 1) {
	        System.err.println("no input");
	        System.exit(1);
	      }
		
		Job3Spark wc = new Job3Spark(args[0]);
		
		JavaPairRDD<Tuple2<String, String>, Integer> result = wc.listCoupleProd();
		result.foreach(y -> System.out.println(y)); 
		
		
  }

  
  

  public JavaPairRDD<String,String> loadData() {

    SparkConf conf = new SparkConf()
        .setAppName("job3spark")
        .setMaster("local[*]");

    @SuppressWarnings("resource")
	JavaSparkContext sc = new JavaSparkContext(conf);
    
    JavaPairRDD<String, String> usersProds = sc.textFile(pathToFile)
			 .mapToPair(line -> new Tuple2<String,String>(ParserJob3.parseReview(line)._1(),ParserJob3.parseReview(line)._2()));
    
	return usersProds;
  }
  
  
  public JavaPairRDD<Tuple2<String, String>, Integer> listCoupleProd() {
	  
    JavaPairRDD<String,String> userProductCouple = loadData();

    JavaPairRDD<String, Iterable<String>> aggrUserProduct = userProductCouple.groupByKey();
    JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> userListProd = aggrUserProduct.join(aggrUserProduct);
    
    JavaPairRDD<Tuple2<String,String>,Integer> userProd1 = userListProd.flatMapToPair(block ->
    {
    	
    	List<Tuple2<Tuple2<String,String>,Integer>> prodCouples = new LinkedList<Tuple2<Tuple2<String,String>,Integer>>();
    	
    	for(String prod_i: block._2()._2()) {
			for(String prod_j:block._2()._2()) {
				if (prod_i.compareTo(prod_j)<0) {
					Tuple2<String,String> tupleTmp = new Tuple2<String,String>(prod_i,prod_j);

					if(!prodCouples.add(new Tuple2<Tuple2<String,String>,Integer>(tupleTmp,1)));	
				}
			}
		}	
		return prodCouples.iterator();
    });
    		
    
    JavaPairRDD<Tuple2<String,String>,Integer> result = userProd1.reduceByKey((a,b) -> a+b).sortByKey(new TupleComparator(),true,1);
    
    return result;
  }

  
  
  
  private static class TupleComparator implements Comparator<Tuple2<String,String>>, Serializable {
		/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

		@Override
	    public int compare(Tuple2<String,String> tuple1, Tuple2<String,String> tuple2) {

	        if (tuple1._1.compareTo(tuple2._1) == 0) {
	            return tuple1._2.compareTo(tuple2._2);
	        }
	        return tuple1._1.compareTo(tuple2._1);
	    }
	}

  
}