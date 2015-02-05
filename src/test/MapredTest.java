package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import task1.Task1Mapper;
import task1.Task1Reducer;

public class MapredTest extends Configured implements Tool {
	
	private Configuration conf;
	
	public MapredTest(Configuration conf) {
		this.conf = conf;
	}
	
	public int run(String[] args) throws Exception {	
		Job job = new Job(conf, "Task1");
		job.setJarByClass(MapredTest.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(Task1Mapper.class);
		job.setReducerClass(Task1Reducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.addResource("core-site.xml");
		
		//conf.addResource(new Path("/users/level4/1103834s/Desktop/bd4-hadoop/conf/core-site.xml"));
		conf.set("mapred.jar", "file:///users/level4/1103834s/Desktop/task1.jar");
		
		MapredTest test = new MapredTest(conf);
		test.run(args);
		
		//System.exit(ToolRunner.run(conf, new MapredTest(conf), args));
	}

}