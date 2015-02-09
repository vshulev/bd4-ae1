package task2;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CountMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable>{
	
	private final static IntWritable one = new  IntWritable(1);
	@SuppressWarnings("unused")
	private Text word = new Text();
	private String[] words;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line = value.toString();
		
		if(line.startsWith("REVISION")){
			words = line.split(" ");
			
			context.write(new  LongWritable(Integer.parseInt(words[1])), one);
		}

	}

}
