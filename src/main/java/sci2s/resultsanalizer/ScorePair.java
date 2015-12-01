package sci2s.resultsanalizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class ScorePair implements WritableComparable<ScorePair> {
	
	protected Text fpid;
	protected DoubleWritable score;
	
	public ScorePair() {
		this.fpid = new Text();
		this.score = new DoubleWritable();
	}
	
	public ScorePair(ScorePair p) {
		this.fpid = new Text(p.fpid);
		this.score = new DoubleWritable(p.score.get());
	}
	
	public ScorePair(String fpid, double score) {
		this.fpid = new Text(fpid);
		this.score = new DoubleWritable(score);
	}
	
	public ScorePair(String fpid, DoubleWritable score) {
		this.fpid = new Text(fpid);
		this.score = score;
	}

	public void set(String fpid, double score) {
		this.fpid = new Text(fpid);
		this.score = new DoubleWritable(score);
	}
	
	public double getScore() {
		return score.get();
	}
	
	public String getFpid() {
		return fpid.toString();
	}

	@Override
	public String toString() {
		return fpid.toString() + ';' + score.toString();
	}

	@Override
	public int hashCode() {
		return fpid.toString().hashCode() + (new Double(score.get())).hashCode();
	}

	public void readFields(DataInput arg0) throws IOException {
		fpid.readFields(arg0);
		score.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		fpid.write(arg0);
		score.write(arg0);
	}

	public int compareTo(ScorePair arg0) {
		
		int res = Double.compare(score.get(), arg0.score.get());
		
		if(res != 0)
			return res;
		
		return fpid.toString().compareTo(arg0.fpid.toString());
	}

}
