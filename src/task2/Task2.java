package task2;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import com.google.common.collect.Lists;

public class Task2 extends Configured implements Tool {

	public static class IntComparator extends WritableComparator {
		public IntComparator() {
			super(IntWritable.class);
		}

		private Integer int1;
		private Integer int2;

		public int compare(byte[] raw1, int offset1, int length1, byte[] raw2,
				int offset2, int length2) {
			int1 = ByteBuffer.wrap(raw1, offset1, length1).getInt();
			int2 = ByteBuffer.wrap(raw2, offset2, length2).getInt();

			return int2.compareTo(int1);
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("startdate", args[2]);
		conf.set("enddate", args[3]);
		conf.setInt("k", Integer.parseInt(args[4]));

		// int numReducers = 10;
		String jobOneInputPath = args[0];
		String jobOneOutputPath = args[1] + "/1result";
		String jobTwoInputPath = jobOneOutputPath;
		String jobTwoOutputPath = args[1] + "/2result";

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
		FileInputFormat.setInputPaths(jobCount, new Path(jobOneInputPath));
		FileOutputFormat.setOutputPath(jobCount, new Path(jobOneOutputPath));

		Job jobSort = Job.getInstance(conf);

		jobSort.setJarByClass(Task2.class);
		jobSort.setJobName("jobSort");

		jobSort.setMapOutputKeyClass(ArtCountPair.class);
		jobSort.setMapOutputValueClass(IntWritable.class);

		jobSort.setOutputValueClass(Text.class);
		jobSort.setMapperClass(SortMapper.class);
		jobSort.setReducerClass(SortReducer.class);

		jobSort.setPartitionerClass(NaturalKeyPartitioner.class);
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

}
