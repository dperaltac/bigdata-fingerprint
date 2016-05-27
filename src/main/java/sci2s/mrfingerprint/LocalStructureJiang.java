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
	public static final int NN = 2;
	public static final double BL = 6*3*NN;
	public static final double W[] = {1, 0.3*180/Math.PI, 0.3*180/Math.PI};
	public static final double BG[] = {8.0, Math.PI/6.0, Math.PI/6.0};
	
	public static final double LOCALBBOX[] = {250, 250, 0.75*Math.PI};
	
	protected double[] fv;
	
	protected Minutia minutia;
	
	public LocalStructureJiang() {
		super();

		fv = new double[3*NN];
		minutia = new Minutia();
	}
	
	public LocalStructureJiang(LocalStructureJiang lsj) {
		super(lsj);

		fv = Arrays.copyOf(lsj.fv, lsj.fv.length);
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
		
		fv = new double[stfv.countTokens()];
		
		int i = 0;
		
		while(stfv.hasMoreTokens()) {
			fv[i] = Double.parseDouble(stfv.nextToken());
			i++;
		}
	}
	
	public LocalStructureJiang(String fpid, int lsid) {

		super(fpid,lsid);
		fv = new double[3*NN];
		minutia = new Minutia();
	}
	
	public LocalStructureJiang(String fpid, int lsid, ArrayList<Minutia> minutiae, int minutia_id, double[] distances, int[] neighbors) {
		
		super(fpid,lsid);
		fv = new double[3*NN];
		
		minutia = new Minutia(minutiae.get(minutia_id));
		
		double angle = minutia.getcrnT();
		int coordx = minutia.getX();
		int coordy = minutia.getY();
		
		for(int k = 0; k < NN; k++) {
			int neighbor = neighbors[k];
			
			// Distance computation
			fv[k] = distances[neighbor];
			
			// Radial angle computation
			fv[k + NN] = Util.dFi(Math.atan2(coordy - minutiae.get(neighbor).getY(), coordx - minutiae.get(neighbor).getX()), angle);
			
			// Minutia direction computation
			fv[k + 2*NN] = Util.dFi(angle, minutiae.get(neighbor).getcrnT());
		}
		
	}
	
	
	public Minutia getMinutia() {
		return new Minutia(minutia);
	}
	
	public static LocalStructureJiang [] extractLocalStructures(String fpid, ArrayList<Minutia> minutiae, double[][] distance_matrix, int[][] neighborhood) {
		
		LocalStructureJiang [] ls = new LocalStructureJiang[minutiae.size()];
		
		for(int i = 0; i < minutiae.size(); i++) {
			ls[i] = new LocalStructureJiang(fpid, i, minutiae, i, distance_matrix[i], neighborhood[i]);
		}
		
		return ls;
	}
	

	
	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);
		
		ArrayPrimitiveWritable ow = new ArrayPrimitiveWritable(fv);
		
		ow.write(out);
		minutia.write(out);
		
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

		super.readFields(in);
		
		ArrayPrimitiveWritable ow = new ArrayPrimitiveWritable(fv);
		
		ow.readFields(in);
		minutia.readFields(in);
		
		fv = (double[]) ow.get();
	}
	
	@Override
	public String toString() {
		
		String result = super.toString() + ";" + minutia.toString() + ";";
		
		for(double i : fv)
			result = result + " " + i;
		
		return result;
	}
	
	

	@Override
	public double similarity(LocalStructure ls) throws LSException {
		
		double sum = 0.0;
		
		if(!(ls instanceof LocalStructureJiang))
			throw new LSException("The similarity can only be computed for local structures of the same type");
		
		LocalStructureJiang lsj = (LocalStructureJiang) ls;
		
		// Check bounding box for the minutiae
		if(Math.abs(minutia.getX()-lsj.minutia.getX()) >= LOCALBBOX[0] ||
				Math.abs(minutia.getY()-lsj.minutia.getY()) >= LOCALBBOX[1] ||
				Math.abs(Util.dFi(minutia.getrT(), lsj.minutia.getrT())) >= LOCALBBOX[2])
			return 0.0;
		
		for(int k = 0; k < NN && sum < BL; k++) {
			sum += Math.abs(fv[k]-lsj.fv[k]) * W[0];
			sum += Math.abs(Util.dFi(fv[k+NN], lsj.fv[k+NN])) * W[1];
			sum += Math.abs(Util.dFi(fv[k+2*NN], lsj.fv[k+2*NN])) * W[2];
		}
		
		if(sum < BL)
			return 1.0 - (sum / BL);
		else
			return 0.0;
	}
	
	
	public double[] transformMinutia(Minutia ref) {

		double [] fg = new double[3];
		
		fg[0] = minutia.getDistance(ref);
		fg[1] = Util.dFi(Math.atan2(minutia.getY() - ref.getY(), minutia.getX() - ref.getX()), ref.getcrnT());
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
