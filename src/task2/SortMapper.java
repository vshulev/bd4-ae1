package task2;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


@SuppressWarnings("unused")
public class SortMapper extends Mapper<LongWritable, Text, ArtCountPair, IntWritable>{
	
	
	private Text word = new Text();
	private String[] words;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		
		words = line.split("\t");
		
		context.write(new ArtCountPair(Long.parseLong(words[0]), Integer.parseInt(words[1])), new IntWritable(1));
		

	}

}
