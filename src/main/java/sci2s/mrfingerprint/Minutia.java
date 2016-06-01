package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 */

/**
 * @author Daniel Peralta <dperalta@decsai.ugr.es>
 *
 */
public class Minutia implements WritableComparable<Minutia>, java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected int index;
	protected int x;
	protected int y;
	protected byte theta;
	protected byte quality;
	
	public Minutia() {
		index = 0;
		x = 0;
		y = 0;
		theta = 0;
		quality = 0;
	}
	
	public Minutia(String value) throws LSException {
		StringTokenizer st = new StringTokenizer(value, " ");
			
		// The line must have 5 elements: index, x, y, theta, quality
		if(st.countTokens() != 5) {
			throw new LSException("Minutia(String): error when reading \"" + value + "\": it has " + st.countTokens() + " instead of 4");
		}

		index = Integer.parseInt(st.nextToken());
		x = Integer.parseInt(st.nextToken());
		y = Integer.parseInt(st.nextToken());
		theta = Byte.parseByte(st.nextToken());
		quality = Byte.parseByte(st.nextToken());
	}
	
	public Minutia(Minutia min) {
		index = min.index;
		x = min.x;
		y = min.y;
		theta = min.theta;
		quality = min.quality;
	}
	
	public Minutia(int index, int x, int y, float theta, byte quality) {
		this.index = index;
		this.x = x;
		this.y = y;
		this.theta = (byte) (theta*256/360 - 128);
		this.quality = quality;
	}
	
	public int getIndex() {
		return index;
	}
	
	public int getX() {
		return x;
	}
	
	public int getY() {
		return y;
	}
	
	public float getTheta() {
		return (theta+128) * 360 / 256;
	}
	
	public float getcT() {
		return 360-getTheta();
	}
	
	public float getcnT() {
		return getcT() - 180;
	}
	
	public float getcrnT() {
		return getcnT() * Util.REGTORAD;
	}
	
	public float getrT() {
		return getTheta() * Util.REGTORAD;
	}
	
	public byte getbT() {
		return theta;
	}
	
	public byte getcbT() {
		return (byte) (256-theta);
	}
	
	public byte getQuality() {
		return quality;
	}
	
	public float getDistance(Minutia m) {
		return (float) Math.sqrt(getSDistance(m));
	}
	
	public float getSDistance(Minutia m) {
		return Util.square(m.x-x) + Util.square(m.y-y);
	}
	
	public float getDistance(float ox, float oy) {
		return (float) Math.sqrt(getSDistance(ox, oy));
	}
	
	public float getSDistance(float ox, float oy) {
		return Util.square(ox-x) + Util.square(oy-y);
	}
	

	
	@Override
	public String toString() {
		return "" + index + " " + x + " " + y + " " + theta + " " + quality;
	}
	
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(index);
		out.writeInt(x);
		out.writeInt(y);
		out.writeByte(theta);
		out.writeByte(quality);
		
	}
	
	public void readFields(DataInput in) throws IOException {

		index = in.readInt();
		x = in.readInt();
		y = in.readInt();
		theta = in.readByte();
		quality = in.readByte();
	}

	public int compareTo(Minutia o) {
		int res;
		
		if((res = Integer.compare(index, o.index)) != 0)
			return res;
		else if((res = Integer.compare(x, o.x)) != 0)
			return res;
		else if((res = Integer.compare(y, o.y)) != 0)
			return res;
		else if((res = Float.compare(theta, o.theta)) != 0)
			return res;
		else
			return Integer.compare(quality, o.quality);
	}
}
