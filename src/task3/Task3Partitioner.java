package task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class Task3Partitioner extends Partitioner<RevDatePair, Text> {

	HashPartitioner<LongWritable, Text> hashPartitioner;
	
	public Task3Partitioner() {
		hashPartitioner = new HashPartitioner<LongWritable, Text>();
	}
	
	@Override
	public int getPartition(RevDatePair key, Text value, int numPartitions) {
		return hashPartitioner.getPartition(new LongWritable(key.getArticleId()), value, numPartitions);
	}

}
