package task2;

import java.util.List;

import main.MyJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Lists;

public class Task2 extends Configured implements MyJob {

	private String outputDir;
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("startdate", args[0]);
		conf.set("enddate", args[1]);
		conf.setInt("k", Integer.parseInt(args[2]));

		String jobOneInputPath = "/user/bd4-ae1/enwiki-20080103-largersample.txt";
		String jobOneOutputPath = "team_d-task2-output1";
		String jobTwoInputPath = jobOneOutputPath;
		String jobTwoOutputPath = "team_d-task2-output2";
		outputDir = jobTwoOutputPath;

		// first job
		Job jobCount = Job.getInstance(conf);

		jobCount.setJarByClass(Task2.class);
		jobCount.setJobName("jobCount");

		jobCount.setMapOutputKeyClass(LongWritable.class);
		jobCount.setMapOutputValueClass(IntWritable.class);

		jobCount.setOutputKeyClass(Text.class);
		jobCount.setOutputValueClass(Text.class);
		jobCount.setMapperClass(CountMapper.class);
		jobCount.setReducerClass(CountReducer.class);
		jobCount.setInputFormatClass(TextInputFormat.class);
		jobCount.setOutputFormatClass(TextOutputFormat.class);
		jobCount.setPartitionerClass(MyPartitioner.class);
		FileInputFormat.setInputPaths(jobCount, new Path(jobOneInputPath));
		FileOutputFormat.setOutputPath(jobCount, new Path(jobOneOutputPath));
		
		jobCount.setNumReduceTasks(6);

		// second job
		Job jobSort = Job.getInstance(conf);

		jobSort.setJarByClass(Task2.class);
		jobSort.setJobName("jobSort");

		jobSort.setMapOutputKeyClass(ArtCountPair.class);
		jobSort.setMapOutputValueClass(IntWritable.class);

		jobSort.setOutputValueClass(Text.class);
		jobSort.setMapperClass(SortMapper.class);
		jobSort.setReducerClass(SortReducer.class);

		jobSort.setGroupingComparatorClass(GroupingComparator.class);
		jobSort.setSortComparatorClass(ArtCountPairComparator.class);

		jobSort.setInputFormatClass(TextInputFormat.class);
		jobSort.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(jobSort, new Path(jobTwoInputPath));
		FileOutputFormat.setOutputPath(jobSort, new Path(jobTwoOutputPath));

		List<Job> jobs = Lists.newArrayList(jobCount, jobSort);
		int exitstatus = 0;

		for (Job job : jobs) {
			boolean jobSuccessful = job.waitForCompletion(true);

			if (!jobSuccessful) {
				System.out.println("Error with job " + job.getJobName() + " ");
				exitstatus = 1;
				break;
			}
		}

		return exitstatus;
	}

	@Override
	public String getOuputDir() {
		return outputDir;
	}

}
