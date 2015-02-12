package task3;

import main.JobRunner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task3Partitioner extends Partitioner<RevDatePair, Text> {
	
	@Override
	public int getPartition(RevDatePair key, Text value, int numPartitions) {
		return (int) (((key.getArticleId() + 0.0) / JobRunner.MAX_ID) * (numPartitions - 1));
	}

}
