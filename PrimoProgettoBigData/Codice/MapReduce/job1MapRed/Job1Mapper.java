package PrimoProgetto.job1MapRed;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	private Text word = new Text();
	private IntWritable yearKey = new IntWritable();
	private String tokens = "[%_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		 String[] parts = value.toString().split(",");
		 
		 try {
			 
		 	long timestamp = Long.parseLong(parts[7]);
			Date date = new Date(timestamp*1000L);
			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			String formattedDate = sdf.format(date);
			int year = Integer.parseInt(formattedDate.substring(6, 10));
		 	
		 	if(year>=1999 && year<=2012) {
		 		yearKey.set(year);	
		 		String cleanLine = parts[8].toLowerCase().replaceAll(tokens, "").replaceAll("  ", " ");
			 	StringTokenizer tokenizer = new StringTokenizer(cleanLine);
			 	
				while (tokenizer.hasMoreTokens()) {
					word.set(tokenizer.nextToken());
					context.write(yearKey,word);
				}
		 	}
		 }
	
		 catch(Exception e) {
			 System.out.println("ERR:"+e);
		 }
	}
	
}

