package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Task1GroupingComparator extends WritableComparator {

	protected Task1GroupingComparator() {
		super(ArticleRevPair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		ArticleRevPair p1 = (ArticleRevPair) w1;
		ArticleRevPair p2 = (ArticleRevPair) w2;
		return p1.getArticleId().compareTo(p2.getArticleId());
	}
	
}
