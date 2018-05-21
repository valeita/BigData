package PrimoProgetto.job1MapRed;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Job1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	private Map<Integer,Map<String,Integer>> mapOccurrence = new TreeMap<Integer,Map<String,Integer>>();
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		Map <String,Integer> mapTmp;	
		if(!mapOccurrence.containsKey(key.get())) {
			
			mapOccurrence.put(key.get(), new TreeMap<String, Integer>());
		}
		mapTmp = mapOccurrence.get(key.get());
			
		for(Text value: values) {
			if(mapTmp.containsKey(value.toString())) {
				
				int cont = mapTmp.get(value.toString());			
				mapTmp.put(value.toString(), (cont+1));
			}
			else {
				mapTmp.put(value.toString(), 1);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		int cont;
		
		for (Entry<Integer, Map<String, Integer>> entry : mapOccurrence.entrySet()){
			
			cont=0;
			
			Map<String,Integer> mapOrdered = new TreeMap<String, Integer>(new ValueComparator(entry.getValue()));
			mapOrdered.putAll(entry.getValue());
			
			for(Entry<String,Integer> entry2 : mapOrdered.entrySet()) {
				
				if(cont>=10) {
					break;
				}
				context.write(new IntWritable(entry.getKey()), new Text("\t" + entry2.getKey() + ": " + entry2.getValue()));
				cont++;
			}
		}
	}
}
