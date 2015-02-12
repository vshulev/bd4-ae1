package task2;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;




public class ArtCountPair implements WritableComparable<ArtCountPair>{
	
	private int count;
	private Long article;
	
	public ArtCountPair( ) {
	}
	
	public ArtCountPair( Long article, int count) {
		super();
		this.count = count;
		this.article = article;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public Long getArticle() {
		return article;
	}
	public void setArticle(Long article) {
		this.article = article;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		article =  Long.parseLong(WritableUtils.readString(in));
		count =  Integer.parseInt(WritableUtils.readString(in));
		
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, String.valueOf(article));
		WritableUtils.writeString(out, String.valueOf(count));
		
	}
	@Override
	public int compareTo(ArtCountPair rc) {
		
		int res = Integer.compare(rc.getCount(), count);	
		
		res = Integer.compare(count, rc.getCount());
		if(res == 0) {
			res = article.compareTo(rc.getArticle());
		}
		return res;
	}
	
	
	

}
