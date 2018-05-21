package PrimoProgetto.job2MapRed;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, YearScoreCount> {
	
	private Integer yearObj;
	private Integer scoreObj;
	private String productID;
	private Map <String,Map<Integer, List<Integer>>> scoreMap = new HashMap<String,Map<Integer, List<Integer>>>();
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] parts = value.toString().split(",");
		 
		try {
			long timestamp = Long.parseLong(parts[7]);
			Date date = new Date(timestamp*1000L);
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String formattedDate = sdf.format(date);
			int year = Integer.parseInt(formattedDate.substring(6, 10));
	 		
	 		productID = parts[1];
	 		scoreObj = Integer.parseInt(parts[6]);
	 		
	 		if(year>=2003 && year<=2012) {
	 			yearObj = year;	
	 			
	 			if(!scoreMap.containsKey(productID)) {
					
					scoreMap.put(productID, new HashMap<Integer, List<Integer>>());
				}
				
				Map<Integer,List<Integer>> mapTmp = scoreMap.get(productID);
				
				if(!mapTmp.containsKey(yearObj)) {

					mapTmp.put(yearObj, new LinkedList<Integer>());
				}
				List<Integer> listTmp = mapTmp.get(yearObj);
				listTmp.add(scoreObj);	
	 		}
	 	}
	 	catch(Exception e) {
	 		
	 	}
	}
	
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for(String product : scoreMap.keySet()){
			
			Map<Integer,List<Integer>>mapTmp = scoreMap.get(product);
			
			for(Integer year : mapTmp.keySet()){
				int sum=0;
				List<Integer> listTmp = mapTmp.get(year);	
				
				for(Integer value: listTmp) sum += value;
				
				YearScoreCount ysc = new YearScoreCount(new IntWritable(year),new IntWritable(sum),new IntWritable(listTmp.size()));
		
				context.write(new Text(product), ysc);
				
				
			}		
		}
	}
	
	
}
