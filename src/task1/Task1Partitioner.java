package task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task1Partitioner extends Partitioner<ArticleRevPair, LongWritable> {

	private static long MAX_ID = 15070781;
	
	@Override
	public int getPartition(ArticleRevPair key, LongWritable value, int numPartitions) {
		return (int) (((key.getArticleId() + 0.0) / MAX_ID) * numPartitions);
	}
	
}
