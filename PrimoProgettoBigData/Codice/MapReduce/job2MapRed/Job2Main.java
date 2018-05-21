package PrimoProgetto.job2MapRed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2Main {
	
	public static void main(String[] args) throws Exception {
		
	Job job = new Job(new Configuration(), "Job2Main");
	job.setJarByClass(Job2Main.class);
	job.setMapperClass(Job2Mapper.class);
	job.setReducerClass(Job2Reducer.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(YearScoreCount.class);
	
	job.waitForCompletion(true);
	}
		
		
}