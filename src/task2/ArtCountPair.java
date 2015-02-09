package task2;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;




public class ArtCountPair implements WritableComparable<ArtCountPair>{
	
	private int count;
	private Long revision;
	
	public ArtCountPair( ) {
	}
	
	public ArtCountPair( Long revision, int count) {
		super();
		this.count = count;
		this.revision = revision;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public Long getRevision() {
		return revision;
	}
	public void setRevision(Long revision) {
		this.revision = revision;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		revision =  Long.parseLong(WritableUtils.readString(in));
		count =  Integer.parseInt(WritableUtils.readString(in));
		
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, String.valueOf(revision));
		WritableUtils.writeString(out, String.valueOf(count));
		
	}
	@Override
	public int compareTo(ArtCountPair rc) {
		
		int res = Integer.compare(rc.getCount(), count);	
		
		res = Integer.compare(count, rc.getCount());
		if(res == 0) {
			res = revision.compareTo(rc.getRevision());
		}
		return res;
	}
	
	
	

}
