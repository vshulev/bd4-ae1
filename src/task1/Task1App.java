package task1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


@SuppressWarnings("unused")
public class Task1App extends Configured implements Tool {
	
	
	static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, LongWritable>{
		private final static  LongWritable one = new  LongWritable(1);
		private Text word = new Text();
		private String[] words;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			
			if(line.startsWith("REVISION")){
				words = line.split(" ");
				
				context.write(new  LongWritable(Integer.parseInt(words[1])), new  LongWritable(Integer.parseInt(words[2])));
			}

		}
		
	}
	
	public static class MyReducer extends Reducer< LongWritable, LongWritable,  LongWritable, Text>{
		public void reduce(LongWritable key, Iterable< LongWritable> values, Context context) throws IOException, InterruptedException{
			
			int sum = 0;
			StringBuilder sb = new StringBuilder();
			List vals = new ArrayList();
			
			
			for( LongWritable value: values){
				sum += 1;
				//sum += value.get();
				//sb.append(value.toString() + " ");
				vals.add(value.toString());
			}
			
			Collections.sort(vals);
			
			for(int v = 0; v < vals.size(); v ++)
				sb.append(vals.get(v) + " ");
			
			context.write(key, new Text(sum + " " +sb.toString()));
			//if(key.get() == 12) context.setStatus("<<<<<<HEREEEEEEEE>>>>>" + key.toString() + "  " + sum);
			
			
		}
	}
	
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

	      return int1.compareTo(int2);
	    }

	  }
	
	
	
	public int run(String[] args) throws Exception{
		
		Job job = new Job(getConf());
		job.setJobName("Task1");
		job.setJarByClass(Task1App.class);
		job.setMapperClass(Map.class);
		
		job.setMapOutputKeyClass( LongWritable.class);
		job.setMapOutputValueClass( LongWritable.class);
		
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(MyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//job.setSortComparatorClass(IntComparator.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		job.submit();
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		//conf.addResource(new Path("file:///users/level4/1006198k/bd4-hadoop/conf/core-site.xml"));
		conf.addResource("file:///users/level4/1006198k/bd4-hadoop/conf/core-site.xml");
		conf.set("mapred.jar", "file:///users/level4/1006198k/BD4/tasks/task1/Task1/task1j.jar");
		System.exit(ToolRunner.run(conf, new Task1App(), args));
	}

}
