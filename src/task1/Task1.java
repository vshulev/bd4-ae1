package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import main.MyJob;

public class Task1 extends Configured implements MyJob {
	
	private String outputDir;
	
	public int run(String[] args) throws Exception {
		outputDir = "team_d-task1-output";
		
		Configuration conf = getConf();
		conf.set("startdate", args[0]);
		conf.set("enddate", args[1]);
		
		Job job = new Job(conf, "Task1");
		job.setJarByClass(Task1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Task1Mapper.class);
		job.setReducerClass(Task1Reducer.class);
		job.setMapOutputKeyClass(ArticleRevPair.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(Task1Partitioner.class);
		job.setGroupingComparatorClass(Task1GroupingComparator.class);
		job.setSortComparatorClass(ArticleRevPairComparator.class);
		
		//job.setNumReduceTasks(6);
		
		FileInputFormat.addInputPath(job, new Path("/user/bd4-ae1/enwiki-20080103-largersample.txt"));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	@Override
	public String getOuputDir() {
		return outputDir;
	}
	
}
