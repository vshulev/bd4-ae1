package task2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;



public class GroupingComparator extends WritableComparator {
	
	protected  GroupingComparator() {
		super(ArtCountPair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArtCountPair p1 = (ArtCountPair) w1;
		ArtCountPair p2 = (ArtCountPair) w2;
		return p1.getRevision().compareTo(p2.getRevision());
	}

}
