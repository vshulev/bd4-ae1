package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ArticleRevPairComparator extends WritableComparator {

	protected ArticleRevPairComparator() {
		super(ArticleRevPair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArticleRevPair p1 = (ArticleRevPair) w1;
		ArticleRevPair p2 = (ArticleRevPair) w2;
		
		int res = p1.getArticleId().compareTo(p2.getArticleId());
		if (res == 0) {
			res = p1.getRevId().compareTo(p2.getRevId());
		}
		return res;
	}
	
}
