package task2;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


@SuppressWarnings("unused")
public class SortReducer extends Reducer< ArtCountPair,  IntWritable, LongWritable, Text>{
	
	public void reduce(ArtCountPair key, Iterable< IntWritable> values, Context context) throws IOException, InterruptedException{
		 
		
		context.write(new LongWritable(key.getArticle()), new Text(String.valueOf(key.getCount())));
	}

}
