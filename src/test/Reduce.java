package test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		String revs = "";
		for (LongWritable value : values) {
			revs.concat(value.toString());
		}
		context.write(key, new Text(revs));
	}

}
