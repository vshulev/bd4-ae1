package task1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer =  new StringTokenizer(line);
		if(tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			if(word.equals("REVISION")) {
				long articleId = Long.parseLong(tokenizer.nextToken());
				long revId = Long.parseLong(tokenizer.nextToken());
				context.write(new LongWritable(articleId), new LongWritable(revId));
			}
		}
	}

}