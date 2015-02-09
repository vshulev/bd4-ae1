package task2;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


@SuppressWarnings("unused")
public class CountReducer extends Reducer< LongWritable, IntWritable,  LongWritable, Text>{
	public void reduce(LongWritable key, Iterable< IntWritable> values, Context context) throws IOException, InterruptedException{
		
		int sum = 0;

		for( IntWritable value: values){
			sum += value.get();
		}
		
		context.write(key, new Text(String.valueOf(sum)));
		
		
	}

}
