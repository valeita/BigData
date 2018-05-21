package PrimoProgetto.job3MapRed;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job3Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private String userID;
	private String productID;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] parts = value.toString().split(",");
		
		productID = parts[1];
		userID = parts[2];
		
		context.write(new Text(userID),new Text(productID));
		
	}
	
	
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
	
	
}
