package task2;
import java.io.IOException;
import java.util.Calendar;

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
		Calendar sdate = javax.xml.bind.DatatypeConverter.parseDateTime(context.getConfiguration().get("startdate"));
		Calendar edate = javax.xml.bind.DatatypeConverter.parseDateTime(context.getConfiguration().get("enddate"));
		
		
		String line = value.toString();
		
		if(line.startsWith("REVISION")){
			words = line.split(" ");
			
			Calendar tempdate = javax.xml.bind.DatatypeConverter.parseDateTime(words[4]);
			
			if(tempdate.after(sdate) && tempdate.before(edate) )
				context.write(new  LongWritable(Integer.parseInt(words[1])), one);
		}

	}

}
