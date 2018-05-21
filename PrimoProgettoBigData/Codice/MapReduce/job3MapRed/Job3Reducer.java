package PrimoProgetto.job3MapRed;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class Job3Reducer extends Reducer<Text, Text, Text, IntWritable> {
	
	private Map<String,Integer> prodCouples = new TreeMap<String,Integer>();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		List<String> prodList = new LinkedList<String>();
		
		for(Text value: values) {	
			prodList.add(value.toString());
		}
		
		for(String prod_i: prodList) {
			for(String prod_j:prodList) {
				if (prod_i.compareTo(prod_j)<0) {
					String tmp = prod_i+","+prod_j;
					if(!prodCouples.containsKey(tmp)) prodCouples.put(tmp, new Integer(1));
					else prodCouples.put(tmp, prodCouples.get(tmp)+1);
				}
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		for(Entry<String, Integer> coupleCount : prodCouples.entrySet()) {
			context.write(new Text(coupleCount.getKey()), new IntWritable(coupleCount.getValue()));
		}	
	}
}