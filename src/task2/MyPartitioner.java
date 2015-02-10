package task2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;




public class MyPartitioner extends Partitioner<LongWritable, IntWritable>{
	
	private long ID = 15070781;
	
	public int getPartition(LongWritable key, IntWritable value, int numPartitions){
		
		long keyid = key.get();
		
		return (int)Math.floor((float)((keyid + 0.0)/ ID) * (numPartitions - 1));
	}

}