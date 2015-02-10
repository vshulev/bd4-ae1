package task3;

import main.MyJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3 extends Configured implements MyJob {
	
	private String outputDir;
	
	public int run(String[] args) throws Exception {
		outputDir = "team_d-task3-output";
		
		Configuration conf = getConf();
		conf.set("date", args[0]);
		
		Job job = new Job(conf, "Task3");
		job.setJarByClass(Task3.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Task3Mapper.class);
		job.setReducerClass(Task3Reducer.class);
		job.setMapOutputKeyClass(RevDatePair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(Task3Partitioner.class);
		job.setGroupingComparatorClass(Task3GroupingComparator.class);
		job.setSortComparatorClass(RevDatePairComparator.class);
		
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
