package task3;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Calendar maxDate = null;
		String maxDateStr = "";
		String maxRevStr = "";
		for(Text value : values) {
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String revStr = tokenizer.nextToken();
			String dateStr = tokenizer.nextToken();
			Calendar date = javax.xml.bind.DatatypeConverter.parseDateTime(dateStr);
			if(maxDate == null || maxDate.before(date)) {
				maxDate = date;
				maxDateStr = dateStr;
				maxRevStr = revStr;
			}
		}
		context.write(key, new Text(maxRevStr + " " + maxDateStr));
	}
	
}
