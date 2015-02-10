package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class Task3 extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("date", args[2]);
		
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
		FileOutputFormat.setOutputPath(job, new Path("team_d-task3-output"));
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
}
