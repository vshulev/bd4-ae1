package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {

	private Configuration conf;
	
	public Task1(Configuration conf) {
		this.conf = conf;
	}
	
	public int run(String[] args) throws Exception {	
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
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("startdate", args[2]);
		conf.set("enddate", args[3]);
		//conf.addResource(new Path("/users/level4/1103834s/Desktop/bd4-hadoop/conf/core-site.xml"));
		//conf.set("mapred.jar", "file:///users/level4/1103834s/Desktop/task1.jar");
		
		Task1 task1 = new Task1(conf);
		task1.run(args);
	}
	
}
