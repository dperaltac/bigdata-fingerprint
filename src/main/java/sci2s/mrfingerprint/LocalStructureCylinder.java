package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;


public class LocalStructureCylinder extends LocalStructure {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final int NS = 8;
	public static final int ND = 6;
	public static final int R = 70;
//	public static final double SIGMAS = 9.333333;
	public static final int OMEGA = 50;
	public static final float MINME = 0.6f; //!< Minimum number of matching elements in two matchable cylinders
	public static final float RELSHIFT = 0.5f*(NS+1);
	public static final float MINVC = 0.75f;
	public static final int NEIGHBORHOODRADIUS = 28;
	public static final int RNEIGHBORHOODRADIUS = R+NEIGHBORHOODRADIUS;
	public static final int MINM = 4;
	public static final int NUMCELLS = ND*NS*NS;
	public static final int MAXNVCELLS = (int) Math.floor((1.0-MINVC*(2-4/Math.PI)) * NUMCELLS);
	public static final float ISIGMAD = 1.43239448783f; //1.0/SIGMAD;
	public static final float DELTAD = 1.047197551f; //(2.0*PI)/Nd;
	public static final float GAUSSIANDEN = 0.042743816f; //1.0 / (SIGMAS * sqrt(2*PI));
	public static final float ISIGMASQ2 = 0.005739796f; //1.0 / (2.0*SIGMAS*SIGMAS);
	public static final float MUPSI = 0.01f; //!< Sigmoid parameter 1 for function Psi
	public static final float TAUPSI = 400f; //!< Sigmoid parameter 2 for function Psi
	public static final float DELTAS = (2.0f*R)/NS;
	public static final float DELTAZETA = 1.57079633f;
	public static final byte DELTAZETABYTE = 64;
	public static final int MINCELLS = (int)Math.floor(MINME*NUMCELLS);
	
	protected float[] cm_vector;
	
	protected Minutia minutia;
	protected boolean valid;
	protected float norm;
	
	public LocalStructureCylinder() {
		super();

		cm_vector = new float[NUMCELLS];
		minutia = new Minutia();
		valid = false;
		norm = 0;
	}
	
	public LocalStructureCylinder(LocalStructureCylinder lsc) {
		super(lsc);

		cm_vector = Arrays.copyOf(lsc.cm_vector, lsc.cm_vector.length);
		minutia = new Minutia(lsc.minutia);
		valid = lsc.valid;
		norm = lsc.norm;
	}
	
//	public LocalStructureCylinder(String value) throws LSException {
//
//		StringTokenizer st = new StringTokenizer(value, ";");
//		
//		// The first two elements are the FPID and LSID
//		fpid = st.nextToken();
//		lsid = Integer.parseInt(st.nextToken());
//		
//		// Then goes the minutia
//		minutia = new Minutia(st.nextToken());
//	
//	// Then goes the validity
//	valid = Boolean.parseBoolean(st.nextToken());
//	
//	// Then goes the norm
//	norm = Float.parseFloat(st.nextToken());
//		
//		// Now only the feature vector remains
//		StringTokenizer stfv = new StringTokenizer(st.nextToken(), " ");
//		
//		// The line must have NUMCELLS elements
//		if(stfv.countTokens() != NUMCELLS) {
//			throw new LSException("LocalStructureCylinder(String): error when reading \"" + stfv.toString() + "\": it has " + stfv.countTokens() + " instead of NUMCELLS = " + NUMCELLS);
//		}
//		
//		cm_vector = new float[stfv.countTokens()];
//		
//		int i = 0;
//		
//		while(stfv.hasMoreTokens()) {
//			cm_vector[i] = Float.parseFloat(stfv.nextToken());
//			i++;
//		}
//		
//		computeNorm();
//	}
	
	public LocalStructureCylinder(String fpid, int lsid) {

		super(fpid,lsid);

		cm_vector = new float[NUMCELLS];
		minutia = new Minutia();
		valid = false;
		norm = 0;
	}
	
	public LocalStructureCylinder(String fpid, int lsid, ArrayList<Minutia> minutiae, int minutia_id) {
		
		super(fpid,lsid);
		cm_vector = new float[NUMCELLS];
		valid = false;
		
		minutia = new Minutia(minutiae.get(minutia_id));
		
		Point [] convex = MonotoneChain.convex_hull(minutiae);
		
		int count = 0;
				
		for (int i=1, pos = 0; i<=NS; i++)
			for (int j=1; j<=NS; j++)
			{
				float [] abscoord = pij(i,j);
				
				if (xiM(abscoord[0],abscoord[1],convex))
				{
					for (int k=1; k<=ND; k++, pos++)
						cm_vector[pos] = cm(abscoord[0],abscoord[1],k,minutiae,convex);
				}
				else
				{
					for (int k=1; k<=ND; k++, pos++)
						cm_vector[pos] = -1;

					count += ND;
				}
			}
		
		if (count >= MAXNVCELLS)
		{
			valid = false;
		}
		else
		{
			count = 0;
			float coordx = minutia.getX();
			float coordy = minutia.getY();
			for (int jj=0; count < MINM && (count+minutiae.size()-jj) >= MINM && jj<minutiae.size(); jj++)
				if (minutiae.get(jj).getSDistance(coordx, coordy) <= RNEIGHBORHOODRADIUS*RNEIGHBORHOODRADIUS)
					count++;

			valid = (count >= MINM);
		}
		
		computeNorm();
	}
	
	protected void computeNorm() {
		
		double sum = 0;
		
		for (int i=0; i<NUMCELLS; i++)
			if(cm_vector[i] >= 0)
				sum += Util.square(cm_vector[i]);
		
		norm = (float) Math.sqrt(sum);
	}
	
	public float cm(float i, float j, int k, ArrayList<Minutia> minutiae, Point[] convex)
	{
		float sum = 0.0f;
		byte angle = minutia.getbT();
		byte dfikk = dFikb(k);
		
		for (Minutia min : minutiae)
			if (minutia.index != min.index && min.getSDistance(i,j) <= NEIGHBORHOODRADIUS*NEIGHBORHOODRADIUS)
				sum += contributionS(min, i, j) * contributionD(min, angle, dfikk);
			
		return Util.psi(sum, MUPSI, TAUPSI);
	}
	

	public float [] pij(int i, int j)
	{
		float relative_i = i - RELSHIFT;
		float relative_j = j - RELSHIFT;
		float cosT = (float) Math.cos(minutia.getrT());
		float sinT = (float) Math.sin(minutia.getrT());
		
		float [] res = new float[2];

		res[0] = DELTAS * (cosT*relative_i + sinT*relative_j) + minutia.getX();
		res[1] = DELTAS * (cosT*relative_j - sinT*relative_i) + minutia.getY();
		
		return res;
	}

	public boolean xiM(float i, float j, Point [] convex)
	{
	    if (minutia.getSDistance(i,j) > R*R) {
	        return false;
	    } else {
	        if (pnpoly(convex,i,j)) {
	            return true;
	        } else {

	            int last = convex.length - 1;
	            
	            if (Util.DistanceFromLine(i,j,convex[last].x,convex[last].y,convex[0].x,convex[0].y) > OMEGA) {
	                for (int k=0; k<last; k++) {
	                    if (Util.DistanceFromLine(i,j,convex[k].x,convex[k].y,convex[k+1].x,convex[k+1].y) <= OMEGA) {
	                        return true;
	                    }
	                }

	                return false;
	            } else {
	                return true;
	            }
	        }
	   }
	}

	public static boolean pnpoly(Point [] convex, double testx, double testy)
	{
		double pivote;
		int last = convex.length-1;
		boolean right;

		pivote = (testy-convex[last].y)*(convex[0].x-convex[last].x) -
							(testx-convex[last].x)*(convex[0].y-convex[last].y);

		if(pivote == 0.0)
			return true;

		right = (pivote < 0);

		for (int i=0; i<last; i++) {
			pivote = (testy-convex[i].y)*(convex[i+1].x-convex[i].x) - (testx-convex[i].x)*(convex[i+1].y-convex[i].y);

			if ((pivote < 0 && !right) || (pivote > 0 && right))
				return false;
			else if(pivote == 0)
				return true;
		}

		return true;
	}

    static float dFik(int k)
    {
        return (float) ((k - 0.5f)*DELTAD - Math.PI);
    }

    static byte dFikb(int k)
    {
        return (byte) ((k - 0.5)*256/ND - 128);
    }


    static float contributionS(Minutia min, float p_i, float p_j)
    {
        return (float) (GAUSSIANDEN / Math.exp(min.getSDistance(p_i,p_j)*ISIGMASQ2));
    }


    static float contributionD(Minutia min, float angle, float k_angle)
    {
    	float alpha = Util.dFiMCC(Util.dFiMCC(angle, min.getrT()), k_angle) * ISIGMAD;

    	/*Area of the curve computation*/
    	return Util.doLeft(alpha + 0.5f) - Util.doLeft(alpha - 0.5f);
    }


    static float contributionD(Minutia min, byte angle, byte k_angle)
    {
    	float alpha = (float) (Util.dFi256(Util.dFi256(angle, min.getbT()), k_angle) * ISIGMAD * Math.PI/128);

    	/*Area of the curve computation*/
    	return Util.doLeft(alpha + 0.5f) - Util.doLeft(alpha - 0.5f);
    }
	
	
	public Minutia getMinutia() {
		return new Minutia(minutia);
	}
	
	
	protected Minutia getMinutiaRef() {
		return minutia;
	}


	public static <T extends LocalStructure> T [] extractLocalStructures(Class<T> lsclass, String encoded, boolean discarding) {
		String fpid = decodeFpid(encoded);
		ArrayList<Minutia> minutiae = decodeTextMinutiae(encoded);
		
		return extractLocalStructures(lsclass, fpid, minutiae, discarding);
	}
	
	
	public static LocalStructureCylinder [] extractLocalStructures(String fpid, ArrayList<Minutia> minutiae, boolean discarding) {
		
		LocalStructureCylinder [] ls = new LocalStructureCylinder[minutiae.size()];
		
		for(int i = 0; i < minutiae.size(); i++) {
			ls[i] = new LocalStructureCylinder(fpid, i, minutiae, i);
		}
		
		return ls;
	}
	

	
	@Override
	public void write(DataOutput out) throws IOException {

		super.write(out);
		
		ArrayPrimitiveWritable ow = new ArrayPrimitiveWritable(cm_vector);
		
		ow.write(out);
		minutia.write(out);
		out.writeBoolean(valid);
		out.writeFloat(norm);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

		super.readFields(in);
		
		ArrayPrimitiveWritable ow = new ArrayPrimitiveWritable(cm_vector);
		
		ow.readFields(in);
		cm_vector = (float[]) ow.get();
		
		minutia.readFields(in);
		valid = in.readBoolean();
		norm = in.readFloat();
		
	}
	
	@Override
	public String toString() {
		
		String result = super.toString() + ";" + minutia.toString() + ";" + valid + ";" + norm + ";";
		
		for(float i : cm_vector)
			result = result + " " + i;
		
		return result;
	}
	
	

	@Override
	public float similarity(LocalStructure ls) throws LSException
	{
		int count = 0;
		float norm_diff = 0;
		float ca_b, cb_a;
		
		if(!(ls instanceof LocalStructureCylinder))
			throw new LSException("The similarity can only be computed for local structures of the same type");
		
		LocalStructureCylinder lsc = (LocalStructureCylinder) ls;

		if (Math.abs(Util.dFi256(minutia.getbT(),lsc.minutia.getbT())) > DELTAZETABYTE)
			return 0;
		
		for (int i=0; i<NUMCELLS; i++)
		{
			ca_b = cm_vector[i];
			cb_a = lsc.cm_vector[i];

			if (ca_b>=0 && cb_a>=0)
			{
				count++;
				norm_diff += Util.square(ca_b-cb_a);
			}
		}
			
		//Check if two cylinders are matchable
		if(count >= MINCELLS)
			return (float) (1.0 - (Math.sqrt(norm_diff)/(norm+lsc.norm)));
		else
			return 0;
	}

	@Override
	public ArrayWritable newArrayWritable(LocalStructure[] ails) {
		return new LSCylinderArray(ails);
	}

	@Override
	public ArrayWritable newArrayWritable() {
		return new LSCylinderArray();
	}
	
	public boolean isValid() {
		return valid;
	}



	public static LocalStructureCylinder [][] loadLSMapFile(Configuration conf) {

		String name = conf.get(Util.MAPFILENAMEPROPERTY, Util.MAPFILEDEFAULTNAME);
    	MapFile.Reader lsmapfile = Util.createMapFileReader(conf, name);
    	
    	LocalStructureCylinder [][] result = null;

		WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(lsmapfile.getKeyClass(), conf);

		LSCylinderArray value = (LSCylinderArray) ReflectionUtils.newInstance(lsmapfile.getValueClass(), conf);
		
		try {
			while(lsmapfile.next(key, value)) {
				result = (LocalStructureCylinder [][]) ArrayUtils.add(result,
						Arrays.copyOf(value.get(), value.get().length, LocalStructureCylinder[].class));
			}
		} catch (Exception e) {
			System.err.println("LocalStructureCylinder.loadLSMapFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}
		
		IOUtils.closeStream(lsmapfile);
		
		return result;		
	}

}
