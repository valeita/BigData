package PrimoProgetto.job2MapRed;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job2Reducer extends Reducer<Text, YearScoreCount, Text, Text> {
	
	private Map <String,Map<Integer,YearScoreCount>> scoreMap = new TreeMap<String,Map<Integer,YearScoreCount>>();
	private final static Integer FIRST_YEAR = 2003;
	private final static Integer LAST_YEAR = 2012;
	
	public void reduce(Text key, Iterable<YearScoreCount> values, Context context) throws IOException, InterruptedException {

		if (!scoreMap.containsKey(key.toString())) scoreMap.put(key.toString(), new HashMap<Integer,YearScoreCount>());
		Map<Integer, YearScoreCount> productMap = scoreMap.get(key.toString());
		
		for(YearScoreCount value: values) {	
			
			if (!productMap.containsKey(value.getYear().get())) {
				productMap.put(value.getYear().get(), new YearScoreCount(value.getYear(),value.getScore(),value.getCount()));
			}
			else {
				productMap.get(value.getYear().get()).incScore(value.getScore());
				productMap.get(value.getYear().get()).incCount(value.getCount());
			}	
		}
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
        for (String product: scoreMap.keySet()) {
        	
        	List<String> avgList = new LinkedList<String>();
        	Map<Integer, YearScoreCount> productMap = scoreMap.get(product);
        	
        	for (int year = FIRST_YEAR; year < LAST_YEAR+1; year++) {
        		if(productMap.containsKey(year)) {
        			double sum = productMap.get(year).getScore().get();
            		double count = productMap.get(year).getCount().get();
            		String num = "" + Math.floor(sum/count * 100) / 100;
            		if(num.length()<4) num = Math.floor(sum/count * 100) / 100 + "0";
            		
            		avgList.add(num);
        		}
        		else avgList.add("----");
        						  
        	}
        	
            context.write(new Text(product), new Text(avgList.toString()));
        }	
        
	}
}