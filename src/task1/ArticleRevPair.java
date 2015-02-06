package task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ArticleRevPair implements WritableComparable<ArticleRevPair> {
	
	private Long articleId;
	private Long revId;
	
	public ArticleRevPair() {
	}
	
	public ArticleRevPair(Long articleId, Long revId) {
		this.articleId = articleId;
		this.revId = revId;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Long.parseLong(WritableUtils.readString(in));
		revId = Long.parseLong(WritableUtils.readString(in));
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, articleId.toString());
		WritableUtils.writeString(out, revId.toString());
	}

	@Override
	public int compareTo(ArticleRevPair r) {
		int res = articleId.compareTo(r.getArticleId());
		if(res == 0) {
			res = revId.compareTo(r.getRevId());
		}
		return res;
	}
	
	public Long getArticleId() {
		return articleId;
	}
	
	public Long getRevId() {
		return revId;
	}
	
}
