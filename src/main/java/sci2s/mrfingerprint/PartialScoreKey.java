package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class PartialScoreKey implements WritableComparable<PartialScoreKey> {

	protected Text fpid;
	protected Text fpid_input;
	
	public PartialScoreKey() {
		fpid = new Text("");
		fpid_input = new Text("");
	}
	
	public PartialScoreKey(String fpid, String fpid_input) {
		this.fpid = new Text(fpid);
		this.fpid_input = new Text(fpid_input);
	}
	
	public Text getFpid() {
		return new Text(fpid);
	}
	
	public Text getFpidInput() {
		return new Text(fpid_input);
	}
	
	public void set(String fpid, String fpid_input) {
		this.fpid.set(fpid);
		this.fpid_input.set(fpid_input);
	}
	
	@Override
	public String toString() {
		
		return (fpid.toString() + ";" + fpid_input.toString());
	}

	public void readFields(DataInput arg0) throws IOException {
		fpid.readFields(arg0);
		fpid_input.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		fpid.write(arg0);
		fpid_input.write(arg0);
	}

	public int compareTo(PartialScoreKey o) {
		int res = fpid.compareTo(o.fpid);

		if(res != 0)
			return res;
		
		return fpid_input.compareTo(o.fpid_input);
	}

	@Override
	public int hashCode() {
		return fpid.toString().hashCode() + fpid_input.toString().hashCode();
	}
	
	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public Comparator() {
			super(PartialScoreKey.class);
		}
		
		// I implement a generic comparison that recursively compare strings until the end of the stream
		@Override
		public int compare(byte[] b1, int s1, int l1,
				byte[] b2, int s2, int l2) {
			try {
				
				int textL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int textL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, textL1, b2, s2, textL2);
				
				// If there is already a difference, or it is the end of the stream, return
				if (cmp != 0 || textL1 == l1 || textL2 == l2) {
					return cmp;
				}
				
				// Otherwise, go on comparing
				return compare(b1, s1 + textL1, l1 - textL1,
						b2, s2 + textL2, l2 - textL2);
				
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
	
	
	static {
		WritableComparator.define(PartialScoreKey.class, new Comparator());
	}

}
