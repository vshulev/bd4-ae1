package task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Task3Partitioner extends Partitioner<RevDatePair, Text> {

	private static long MAX_ID = 15070781;
	
	@Override
	public int getPartition(RevDatePair key, Text value, int numPartitions) {
		return (int) (((key.getArticleId() + 0.0) / MAX_ID) * (numPartitions - 1));
	}

}
