package task1;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1Mapper extends Mapper<LongWritable, Text, ArticleRevPair, LongWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		Calendar sdate = javax.xml.bind.DatatypeConverter.parseDateTime(context.getConfiguration().get("startdate"));
		Calendar edate = javax.xml.bind.DatatypeConverter.parseDateTime(context.getConfiguration().get("enddate"));
		
		String line = value.toString();
		StringTokenizer tokenizer =  new StringTokenizer(line);
		if(tokenizer.hasMoreTokens()) {
			String word = tokenizer.nextToken();
			if(word.equals("REVISION")) {
				long articleId = Long.parseLong(tokenizer.nextToken());
				long revId = Long.parseLong(tokenizer.nextToken());
				tokenizer.nextToken();
				Calendar tempdate = javax.xml.bind.DatatypeConverter.parseDateTime(tokenizer.nextToken());
				
				if(tempdate.after(sdate) && tempdate.before(edate) )
					context.write(new ArticleRevPair(articleId, revId), new LongWritable(revId));
			}
		}
	}

}