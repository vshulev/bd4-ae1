package task1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1Reducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		String revs = "";
		for (LongWritable value : values) {
			revs += " " + value.toString();
			count++;
		}
		revs = count + " " + revs;
		context.write(key, new Text(revs));
	}

}