package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;

/**
 * 
 */

/**
 * @author Daniel Peralta <dperalta@decsai.ugr.es>
 *
 */
public class Minutia implements Writable, java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected int index;
	protected int x;
	protected int y;
	protected double theta;
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
		theta = Double.parseDouble(st.nextToken());
		quality = Integer.parseInt(st.nextToken());
	}
	
	public Minutia(Minutia min) {
		index = min.index;
		x = min.x;
		y = min.y;
		theta = min.theta;
		quality = min.quality;
	}
	
	public Minutia(int index, int x, int y, double theta, int quality) {
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
	
	public double getTheta() {
		return theta;
	}
	
	public double getcT() {
		return (theta==0) ? 0 : (360-theta);
	}
	
	public double getcnT() {
		return getcT() - 180;
	}
	
	public double getcrnT() {
		return getcnT() * Util.REGTORAD;
	}
	
	public double getrT() {
		return theta * Util.REGTORAD;
	}
	
	public int getQuality() {
		return quality;
	}
	
	public double getDistance(Minutia m) {
		return Math.sqrt((m.x-x)*(m.x-x) + (m.y-y)*(m.y-y));
	}
	
	public double getSDistance(Minutia m) {
		return (m.x-x)*(m.x-x) + (m.y-y)*(m.y-y);
	}
	
	public double getDistance(double ox, double oy) {
		return Math.sqrt((ox-x)*(ox-x) + (oy-y)*(oy-y));
	}
	
	public double getSDistance(double ox, double oy) {
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
		out.writeDouble(theta);
		out.writeInt(quality);
		
	}
	
	public void readFields(DataInput in) throws IOException {

		index = in.readInt();
		x = in.readInt();
		y = in.readInt();
		theta = in.readDouble();
		quality = in.readInt();
	}
}
