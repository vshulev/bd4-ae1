package task3;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<LongWritable, Text, RevDatePair, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Calendar givenDate = javax.xml.bind.DatatypeConverter.parseDateTime(context.getConfiguration().get("date"));
		
		String line = value.toString();
		StringTokenizer tokenizer =  new StringTokenizer(line);
		if(tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			if(word.equals("REVISION")) {
				long articleId = Long.parseLong(tokenizer.nextToken());
				long revId = Long.parseLong(tokenizer.nextToken());
				tokenizer.nextToken();
				String date = tokenizer.nextToken();
				if(javax.xml.bind.DatatypeConverter.parseDateTime(date).before(givenDate))
					context.write(new RevDatePair(articleId, date), new Text(revId + " " + date));
			}
		}
	}
}
