package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3 {

	private Configuration conf;
	
	public Task3(Configuration conf) {
		this.conf = conf;
	}
	
	public int run(String[] args) throws Exception {	
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
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.addResource(new Path("/users/level4/1103834s/Desktop/bd4-hadoop/conf/core-site.xml"));
		//conf.set("mapred.jar", "file:///users/level4/1103834s/Desktop/task1.jar");
		
		Task3 task3 = new Task3(conf);
		task3.run(args);
	}
	
}
