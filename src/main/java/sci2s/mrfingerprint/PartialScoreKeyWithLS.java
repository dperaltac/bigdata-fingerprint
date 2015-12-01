package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class PartialScoreKeyWithLS implements WritableComparable<PartialScoreKeyWithLS> {

	protected Class<? extends LocalStructure> matcher_class;
	protected Text fpid;
	protected Text fpid_input;
	
	public PartialScoreKeyWithLS() {
		matcher_class = LocalStructure.class;
		fpid = new Text("");
		fpid_input = new Text("");
	}
	
	public PartialScoreKeyWithLS(Class<? extends LocalStructure> matcher_class, String fpid, String fpid_input) {
		this.matcher_class = matcher_class;
		this.fpid = new Text(fpid);
		this.fpid_input = new Text(fpid_input);
	}
	
	public Class<? extends LocalStructure> getMatcherClass() {
		return matcher_class;
	}
	
	public Text getFpid() {
		return new Text(fpid);
	}
	
	public Text getFpidInput() {
		return new Text(fpid_input);
	}
	
	@Override
	public String toString() {
		String res = new String(matcher_class.getName());
		
		return (res + ";" + fpid.toString() + ";" + fpid_input.toString());
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput arg0) throws IOException {
		
		Text classname = new Text();
		
		fpid.readFields(arg0);
		fpid_input.readFields(arg0);
		
		classname.readFields(arg0);
		
		try {
			matcher_class = (Class<? extends LocalStructure>) Class.forName(classname.toString());
		} catch (ClassNotFoundException e) {

			matcher_class = null;
			System.err.println("PartialScoreKey.readFields: class " + classname + " was not found");
			e.printStackTrace();
		}
	}

	public void write(DataOutput arg0) throws IOException {
		
		Text classname = new Text(matcher_class.getName());
		
		fpid.write(arg0);
		fpid_input.write(arg0);
		classname.write(arg0);
	}

	public int compareTo(PartialScoreKeyWithLS o) {
		int res = fpid.compareTo(o.fpid);

		if(res == 0) {
			res = fpid_input.compareTo(o.fpid_input);
			
			if(res == 0)
				matcher_class.getName().compareTo(o.matcher_class.getName());
		}

		return res;
	}

	@Override
	public int hashCode() {
		return fpid.toString().hashCode() + fpid_input.toString().hashCode() + matcher_class.getName().hashCode();
	}
	
	
	
	
	public static class Comparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public Comparator() {
			super(PartialScoreKeyWithLS.class);
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
		WritableComparator.define(PartialScoreKeyWithLS.class, new Comparator());
	}

}
