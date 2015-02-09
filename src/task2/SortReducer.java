package task2;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


@SuppressWarnings("unused")
public class SortReducer extends Reducer< ArtCountPair,  IntWritable, LongWritable, Text>{
	
	//static enum Counters {NUM_RECORDS}
	
	public void reduce(ArtCountPair key, Iterable< IntWritable> values, Context context) throws IOException, InterruptedException{
		 
		
		context.write(new LongWritable(key.getRevision()), new Text(String.valueOf(key.getCount())));
		//context.getCounter(Counters.NUM_RECORDS).increment(1);
	}

}
