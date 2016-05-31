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
	protected float theta;
	protected int quality;
	
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
		theta = Float.parseFloat(st.nextToken());
		quality = Integer.parseInt(st.nextToken());
	}
	
	public Minutia(Minutia min) {
		index = min.index;
		x = min.x;
		y = min.y;
		theta = min.theta;
		quality = min.quality;
	}
	
	public Minutia(int index, int x, int y, float theta, int quality) {
		this.index = index;
		this.x = x;
		this.y = y;
		this.theta = theta;
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
		return theta;
	}
	
	public float getcT() {
		return (theta==0) ? 0 : (360-theta);
	}
	
	public float getcnT() {
		return getcT() - 180;
	}
	
	public float getcrnT() {
		return getcnT() * Util.REGTORAD;
	}
	
	public float getrT() {
		return theta * Util.REGTORAD;
	}
	
	public int getQuality() {
		return quality;
	}
	
	public float getDistance(Minutia m) {
		return (float) Math.sqrt((m.x-x)*(m.x-x) + (m.y-y)*(m.y-y));
	}
	
	public float getSDistance(Minutia m) {
		return (m.x-x)*(m.x-x) + (m.y-y)*(m.y-y);
	}
	
	public float getDistance(float ox, float oy) {
		return (float) Math.sqrt((ox-x)*(ox-x) + (oy-y)*(oy-y));
	}
	
	public float getSDistance(float ox, float oy) {
		return (ox-x)*(ox-x) + (oy-y)*(oy-y);
	}
	

	
	@Override
	public String toString() {
		return "" + index + " " + x + " " + y + " " + theta + " " + quality;
	}
	
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(index);
		out.writeInt(x);
		out.writeInt(y);
		out.writeFloat(theta);
		out.writeInt(quality);
		
	}
	
	public void readFields(DataInput in) throws IOException {

		index = in.readInt();
		x = in.readInt();
		y = in.readInt();
		theta = in.readFloat();
		quality = in.readInt();
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
