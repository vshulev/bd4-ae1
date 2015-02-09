package task2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


@SuppressWarnings("unused")
public class NaturalKeyPartitioner extends Partitioner<ArtCountPair,  IntWritable>{
	
	
	HashPartitioner<LongWritable,  IntWritable> hashPartitioner;
	
	public NaturalKeyPartitioner() {
		hashPartitioner = new HashPartitioner<LongWritable,  IntWritable>();
	}

	@Override
	public int getPartition(ArtCountPair key, IntWritable value, int numPartitions) {
		return hashPartitioner.getPartition(new LongWritable(key.getRevision()), value, numPartitions);
	}

}
