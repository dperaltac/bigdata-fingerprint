package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;


public class LocalStructureJiang extends LocalStructure {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final int NN = 4;
	public static final int BL = 6*3*NN;
//	public static final float W[] = {1, (float)(0.3*180/Math.PI), (float)(0.3*180/Math.PI)};
	public static final float W[] = {1, (float)(0.3*180/128), (float)(0.3*180/128)};
	public static final float BG[] = {8.0f, (float)(Math.PI/6.0), (float)(Math.PI/6.0)};
	
	// Improvements
	public static final int LOCALBBOX[] = {250, 250, 96};
	public static final int MINDIST = 0;
	public static final int MAXDIST = 800000;

	protected float[] fvdist;
	protected byte[] fvangle;
	
	protected Minutia minutia;
	
	public LocalStructureJiang() {
		super();

		fvdist = new float[NN];
		fvangle = new byte[2*NN];
		minutia = new Minutia();
	}
	
	public LocalStructureJiang(LocalStructureJiang lsj) {
		super(lsj);

		fvdist = Arrays.copyOf(lsj.fvdist, lsj.fvdist.length);
		fvangle = Arrays.copyOf(lsj.fvangle, lsj.fvangle.length);
		minutia = new Minutia(lsj.minutia);
	}
	
	public LocalStructureJiang(String value) throws LSException {

		StringTokenizer st = new StringTokenizer(value, ";");
		
		// The first two elements are the FPID and LSID
		fpid = st.nextToken();
		lsid = Integer.parseInt(st.nextToken());
		
		// Then goes the minutia
		minutia = new Minutia(st.nextToken());
		
		// Now only the feature vector remains
		StringTokenizer stfv = new StringTokenizer(st.nextToken(), " ");
		
		// The line must have 3*NN elements
		if(stfv.countTokens() != 3*NN) {
			throw new LSException("LocalStructureJiang(String): error when reading \"" + stfv.toString() + "\": it has " + stfv.countTokens() + " instead of 3*NN = " + 3*NN);
		}

		fvdist = new float[NN];
		fvangle = new byte[2*NN];
		
		int i = 0;
		
		while(stfv.hasMoreTokens()) {
			fvdist[i] = Float.parseFloat(stfv.nextToken());
			fvangle[2*i] = Byte.parseByte(stfv.nextToken());
			fvangle[2*i+1] = Byte.parseByte(stfv.nextToken());
			i++;
		}
	}
	
	public LocalStructureJiang(String fpid, int lsid) {

		super(fpid,lsid);

		fvdist = new float[NN];
		fvangle = new byte[2*NN];
		minutia = new Minutia();
	}
	
	public LocalStructureJiang(String fpid, int lsid, ArrayList<Minutia> minutiae, int minutia_id, float[] distances, int[] neighbors) {
		
		super(fpid,lsid);

		fvdist = new float[NN];
		fvangle = new byte[2*NN];
		
		minutia = new Minutia(minutiae.get(minutia_id));
		
		byte angle = minutia.getcbT();
		int coordx = minutia.getX();
		int coordy = minutia.getY();
		
		for(int k = 0; k < NN; k++) {
			int neighbor = neighbors[k];
			
			// Distance computation
			fvdist[k] = distances[neighbor];
			
			double angle2 = Math.atan2(coordy - minutiae.get(neighbor).getY(), coordx - minutiae.get(neighbor).getX());
			
			// Radial angle computation
			fvangle[k] = Util.dFi256((byte) (angle2*128/Math.PI), angle);
			
			// Minutia direction computation
			fvangle[k + NN] = Util.dFi256(angle, minutiae.get(neighbor).getcbT());
		}
		
	}
	
	
	public Minutia getMinutia() {
		return new Minutia(minutia);
	}

	// Improvement: discard neighborhoods where the minutiae are very far or very close
	@Override
	public boolean isValid() {
		
		for(float d : fvdist)
			if(d > MAXDIST || d < MINDIST)
				return false;
		
		return true;
	}
	
	public static LocalStructureJiang [] extractLocalStructures(String fpid,
			ArrayList<Minutia> minutiae,
			float[][] distance_matrix,
			int[][] neighborhood) {
		
		LocalStructureJiang [] ls = new LocalStructureJiang[minutiae.size()];
		
		for(int i = 0; i < minutiae.size(); i++)
			ls[i] = new LocalStructureJiang(fpid, i, minutiae, i, distance_matrix[i], neighborhood[i]);
		
		return ls;
	}

	
	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);

		ArrayPrimitiveWritable ow1 = new ArrayPrimitiveWritable(fvdist);
		ArrayPrimitiveWritable ow2 = new ArrayPrimitiveWritable(fvangle);

		ow1.write(out);
		ow2.write(out);
		minutia.write(out);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

		super.readFields(in);

		ArrayPrimitiveWritable ow1 = new ArrayPrimitiveWritable(fvdist);
		ArrayPrimitiveWritable ow2 = new ArrayPrimitiveWritable(fvangle);

		ow1.readFields(in);
		ow2.readFields(in);
		minutia.readFields(in);

		fvdist = (float[]) ow1.get();
		fvangle = (byte[]) ow2.get();
	}
	
	@Override
	public String toString() {
		
		String result = super.toString() + ";" + minutia.toString() + ";";

		for(float i : fvdist)
			result = result + " " + i;
		
		for(byte i : fvangle)
			result = result + " " + i;
		
		return result;
	}
	
	

	@Override
	public float similarity(LocalStructure ls) throws LSException {
		
		float sum = 0.0f;
		
		if(!(ls instanceof LocalStructureJiang))
			throw new LSException("The similarity can only be computed for local structures of the same type");
		
		LocalStructureJiang lsj = (LocalStructureJiang) ls;
		
		// Check bounding box for the minutiae
		if(Math.abs(minutia.getX()-lsj.minutia.getX()) >= LOCALBBOX[0] ||
				Math.abs(minutia.getY()-lsj.minutia.getY()) >= LOCALBBOX[1] ||
				Math.abs(Util.dFi256(minutia.getbT(), lsj.minutia.getbT())) >= LOCALBBOX[2])
			return 0.0f;
		
		
		for(int k = 0; k < NN && sum < BL; k++) {
			
			sum += Math.abs(fvdist[k]-lsj.fvdist[k]) * W[0];
			sum += Math.abs(Util.dFi256(fvangle[k]   , lsj.fvangle[k]   )) * W[1];
			sum += Math.abs(Util.dFi256(fvangle[k+NN], lsj.fvangle[k+NN])) * W[2];
		}
		
		if(sum < BL)
			return 1.0f - (sum / BL);
		else
			return 0.0f;
	}
	
	
	public float[] transformMinutia(Minutia ref) {

		float [] fg = new float[3];
		
		fg[0] = minutia.getDistance(ref);
		fg[1] = Util.dFi((float) Math.atan2(minutia.getY() - ref.getY(), minutia.getX() - ref.getX()), ref.getcrnT());
		fg[2] = Util.dFi(minutia.getcrnT(), ref.getcrnT());
		
		return fg;
	}


	@Override
	public ArrayWritable newArrayWritable(LocalStructure[] ails) {
		return new LSJiangArray(ails);
	}

	@Override
	public ArrayWritable newArrayWritable() {
		return new LSJiangArray();
	}


	public static LocalStructureJiang [][] loadLSMapFile(Configuration conf) {

		String name = conf.get(Util.MAPFILENAMEPROPERTY, Util.MAPFILEDEFAULTNAME);
    	MapFile.Reader lsmapfile = Util.createMapFileReader(conf, name);
    	
    	LocalStructureJiang [][] result = null;

		WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(lsmapfile.getKeyClass(), conf);

		LSJiangArray value = (LSJiangArray) ReflectionUtils.newInstance(lsmapfile.getValueClass(), conf);
		
		try {
			while(lsmapfile.next(key, value)) {
				result = (LocalStructureJiang [][]) ArrayUtils.add(result,
						Arrays.copyOf(value.get(), value.get().length, LocalStructureJiang[].class));
			}
		} catch (Exception e) {
			System.err.println("LocalStructureJiang.loadLSMapFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}
		
		IOUtils.closeStream(lsmapfile);
		
		return result;		
	}
}
