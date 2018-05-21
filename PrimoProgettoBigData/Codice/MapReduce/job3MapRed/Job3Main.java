package PrimoProgetto.job3MapRed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3Main {
	
	public static void main(String[] args) throws Exception {
		
	Job job = new Job(new Configuration(), "Job3Main");
	job.setJarByClass(Job3Main.class);
	job.setMapperClass(Job3Mapper.class);
	job.setReducerClass(Job3Reducer.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	job.waitForCompletion(true);
	}
		
		
}