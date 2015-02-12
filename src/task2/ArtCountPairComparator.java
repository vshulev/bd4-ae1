package task2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;




public class ArtCountPairComparator extends WritableComparator{
	
	protected  ArtCountPairComparator() {
		super(ArtCountPair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1,  WritableComparable w2) {

		ArtCountPair p1 = (ArtCountPair) w1;
		ArtCountPair p2 = (ArtCountPair) w2;

		int res = Integer.compare(p2.getCount(), p1.getCount());
		
		if (res == 0) {
			
			res = p1.getArticle().compareTo(p2.getArticle());
		}

		return res;
	}
}
