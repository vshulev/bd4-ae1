package task3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<RevDatePair, Text, LongWritable, Text> {

	public void reduce(RevDatePair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		if(values.iterator().hasNext())
			context.write(new LongWritable(key.getArticleId()), new Text(values.iterator().next()));
	}
	
}
