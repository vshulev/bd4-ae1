package task3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class RevDatePair implements WritableComparable<RevDatePair> {

	private Long articleId;
	private Calendar date;
	private String dateStr;
	
	public RevDatePair() {
		
	}
	
	public RevDatePair(Long articleId, String date) {
		this.articleId = articleId;
		this.dateStr = date;
		this.date = javax.xml.bind.DatatypeConverter.parseDateTime(dateStr);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		articleId = Long.parseLong(WritableUtils.readString(in));
		dateStr = WritableUtils.readString(in);
		date = javax.xml.bind.DatatypeConverter.parseDateTime(dateStr); 
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, articleId.toString());
		WritableUtils.writeString(out, dateStr);
	}

	@Override
	public int compareTo(RevDatePair r) {
		int res = articleId.compareTo(r.getArticleId());
		if(res == 0) {
			res = r.getDate().compareTo(date);
		}
		return res;
	}
	
	public Long getArticleId() {
		return articleId;
	}
	
	public Calendar getDate() {
		return date;
	}

}
