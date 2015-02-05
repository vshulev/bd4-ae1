package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Task3GroupingComparator extends WritableComparator {

	protected Task3GroupingComparator() {
		super(RevDatePair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		RevDatePair p1 = (RevDatePair) w1;
		RevDatePair p2 = (RevDatePair) w2;
		return p1.getArticleId().compareTo(p2.getArticleId());
	}
	
}
