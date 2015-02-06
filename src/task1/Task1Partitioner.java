package task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class Task1Partitioner extends Partitioner<ArticleRevPair, LongWritable> {

	HashPartitioner<LongWritable, LongWritable> hashPartitioner;
	
	public Task1Partitioner() {
		hashPartitioner = new HashPartitioner<LongWritable, LongWritable>();
	}
	
	@Override
	public int getPartition(ArticleRevPair key, LongWritable value, int numPartitions) {
		return hashPartitioner.getPartition(new LongWritable(key.getArticleId()), value, numPartitions);
	}
	
}
