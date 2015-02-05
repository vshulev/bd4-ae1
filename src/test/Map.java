package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer =  new StringTokenizer(line);
		if(tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			if(word.equals("REVISION")) {
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

}
