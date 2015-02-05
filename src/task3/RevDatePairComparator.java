package task3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RevDatePairComparator extends WritableComparator {

	protected RevDatePairComparator() {
		super(RevDatePair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		RevDatePair p1 = (RevDatePair) w1;
		RevDatePair p2 = (RevDatePair) w2;

		// (first check on udid
		int res = p1.getArticleId().compareTo(p2.getArticleId());

		if (res == 0) {
			res = p2.getDate().compareTo(p1.getDate());
		}

		return res;
	}

}
