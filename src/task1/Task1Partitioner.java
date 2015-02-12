package task1;

import main.JobRunner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task1Partitioner extends Partitioner<ArticleRevPair, LongWritable> {
	
	@Override
	public int getPartition(ArticleRevPair key, LongWritable value, int numPartitions) {
		return (int) (((key.getArticleId() + 0.0) / JobRunner.MAX_ID) * (numPartitions - 1));
	}
	
}
