package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;


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
	public static final double MINME = 0.6; //!< Minimum number of matching elements in two matchable cylinders
	public static final double RELSHIFT = 0.5*(NS+1);
	public static final double MINVC = 0.75;
	public static final int NEIGHBORHOODRADIUS = 28;
	public static final int RNEIGHBORHOODRADIUS = R+NEIGHBORHOODRADIUS;
	public static final int MINM = 4;
	public static final int NUMCELLS = ND*NS*NS;
	public static final int MAXNVCELLS = (int) Math.floor((1.0-MINVC*(2-4/Math.PI)) * NUMCELLS);
	public static final double ISIGMAD = 1.43239448783; //1.0/SIGMAD;
	public static final double DELTAD = 1.047197551; //(2.0*PI)/Nd;
	public static final double GAUSSIANDEN = 0.042743816; //1.0 / (SIGMAS * sqrt(2*PI));
	public static final double ISIGMASQ2 = 0.005739796; //1.0 / (2.0*SIGMAS*SIGMAS);
	public static final double MUPSI = 0.01; //!< Sigmoid parameter 1 for function Psi
	public static final double TAUPSI = 400; //!< Sigmoid parameter 2 for function Psi
	public static final double DELTAS = (2.0*R)/NS;
	public static final double DELTAZETA = 1.57079633;
	public static final int MINCELLS = (int)Math.floor(MINME*NUMCELLS);

	
	
	protected double[] cm_vector;
	
	protected Minutia minutia;
	protected boolean valid;
	
	public LocalStructureCylinder() {
		super();

		cm_vector = new double[NUMCELLS];
		minutia = new Minutia();
		valid = false;
	}
	
	public LocalStructureCylinder(LocalStructureCylinder lsc) {
		super(lsc);

		cm_vector = Arrays.copyOf(lsc.cm_vector, lsc.cm_vector.length);
		minutia = new Minutia(lsc.minutia);
		valid = lsc.valid;
	}
	
	public LocalStructureCylinder(String value) throws LSException {

		StringTokenizer st = new StringTokenizer(value, ";");
		
		// The first two elements are the FPID and LSID
		fpid = st.nextToken();
		lsid = Integer.parseInt(st.nextToken());
		
		// Then goes the minutia
		minutia = new Minutia(st.nextToken());
		
		// Then goes the validity
		valid = Boolean.parseBoolean(st.nextToken());
		
		// Now only the feature vector remains
		StringTokenizer stfv = new StringTokenizer(st.nextToken(), " ");
		
		// The line must have NUMCELLS elements
		if(stfv.countTokens() != NUMCELLS) {
			throw new LSException("LocalStructureCylinder(String): error when reading \"" + stfv.toString() + "\": it has " + stfv.countTokens() + " instead of NUMCELLS = " + NUMCELLS);
		}
		
		cm_vector = new double[stfv.countTokens()];
		
		int i = 0;
		
		while(stfv.hasMoreTokens()) {
			cm_vector[i] = Double.parseDouble(stfv.nextToken());
			i++;
		}
	}
	
	public LocalStructureCylinder(String fpid, int lsid) {

		super(fpid,lsid);

		cm_vector = new double[NUMCELLS];
		minutia = new Minutia();
		valid = false;
	}
	
	public LocalStructureCylinder(String fpid, int lsid, ArrayList<Minutia> minutiae, int minutia_id) {
		
		super(fpid,lsid);
		cm_vector = new double[NUMCELLS];
		valid = false;
		
		minutia = new Minutia(minutiae.get(minutia_id));
		
		Point [] convex = MonotoneChain.convex_hull(minutiae);
		
		int count = 0;
				
		for (int i=1, pos = 0; i<=NS; i++)
			for (int j=1; j<=NS; j++)
			{
				double [] abscoord = pij(i,j);
				
				if (xiM(abscoord[0],abscoord[1],convex))
				{
					for (int k=1; k<=ND; k++, pos++)
						cm_vector[pos] = cm(abscoord[0],abscoord[1],k,minutiae,convex);
				}
				else
				{
					for (int k=1; k<=ND; k++, pos++)
						cm_vector[pos] = -1.0;

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
	}
	
	public double cm(double i, double j, int k, ArrayList<Minutia> minutiae, Point[] convex)
	{
		double sum = 0.0;
		double angle = minutia.getrT();
		double dfikk = dFik(k);
		
		for (Minutia min : minutiae)
			if (minutia.index != min.index && min.getSDistance(i,j) <= NEIGHBORHOODRADIUS*NEIGHBORHOODRADIUS)
			{
				double cs = contributionS(min, i, j);
				double cd = contributionD(min, angle, dfikk); 
				sum += cs * cd;
			}
			
		return Util.psi(sum, MUPSI, TAUPSI);
	}
	

	public double [] pij(int i, int j)
	{
		double relative_i = i - RELSHIFT;
		double relative_j = j - RELSHIFT;
		double cosT = Math.cos(minutia.getrT());
		double sinT = Math.sin(minutia.getrT());
		
		double [] res = new double[2];

		res[0] = DELTAS * (cosT*relative_i + sinT*relative_j) + minutia.getX();
		res[1] = DELTAS * (cosT*relative_j - sinT*relative_i) + minutia.getY();
		
		return res;
	}

	public boolean xiM(double i, double j, Point [] convex)
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

    static double dFik(int k)
    {
        return (k - 0.5)*DELTAD - Math.PI;
    }


    static double contributionS(Minutia min, double p_i, double p_j)
    {
        return GAUSSIANDEN / Math.exp(min.getSDistance(p_i,p_j)*ISIGMASQ2);
    }


    static double contributionD(Minutia min, double angle, double k_angle)
    {
    	double alpha = Util.dFiMCC(Util.dFiMCC(angle, min.getrT()), k_angle) * ISIGMAD;

    	/*Area of the curve computation*/
    	return Util.doLeft(alpha + 0.5) - Util.doLeft(alpha - 0.5);
    }
	
	
	public Minutia getMinutia() {
		return new Minutia(minutia);
	}


	public static <T extends LocalStructure> T [] extractLocalStructures(Class<T> lsclass, String encoded) {
		String fpid = decodeFpid(encoded);
		ArrayList<Minutia> minutiae = decodeTextMinutiae(encoded);
		
		return extractLocalStructures(lsclass, fpid, minutiae);
	}
	
	
	public static LocalStructureCylinder [] extractLocalStructures(String fpid, ArrayList<Minutia> minutiae) {
		
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
		BooleanWritable vw = new BooleanWritable(valid);
		
		ow.write(out);
		minutia.write(out);
		vw.write(out);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {

		super.readFields(in);
		
		ArrayPrimitiveWritable ow = new ArrayPrimitiveWritable(cm_vector);
		BooleanWritable vw = new BooleanWritable(valid);
		
		ow.readFields(in);
		minutia.readFields(in);
		vw.readFields(in);
		
		cm_vector = (double[]) ow.get();
		valid = vw.get();
	}
	
	@Override
	public String toString() {
		
		String result = super.toString() + ";" + minutia.toString() + ";" + valid + ";";
		
		for(double i : cm_vector)
			result = result + " " + i;
		
		return result;
	}
	
	

	@Override
	public double similarity(LocalStructure ls)
	{
		int count = 0;
		double norma_b = 0, normb_a = 0, norm_diff = 0;
		double ca_b, cb_a;
		
//		if(!(ls instanceof LocalStructureCylinder))
//			throw new LSException("The similarity can only be computed for local structures of the same type");
		
		LocalStructureCylinder lsc = (LocalStructureCylinder) ls;

		if (Math.abs(Util.dFiMCC(minutia.getrT(),lsc.minutia.getrT())) > DELTAZETA)
			return 0;
		
		for (int i=0; i<NUMCELLS; i++)
		{
			ca_b = cm_vector[i];
			cb_a = lsc.cm_vector[i];

			if (ca_b>=0 && cb_a>=0)
			{
				count++;

				norma_b += ca_b*ca_b;
				normb_a += cb_a*cb_a;
				norm_diff += ca_b*cb_a;
			}
		}
			
		//Check if two cylinders are matchable
		if(count >= MINCELLS)
		{
			norm_diff = Math.sqrt(norma_b + normb_a - 2.0*norm_diff);
			return 1.0 - (norm_diff/(Math.sqrt(norma_b)+Math.sqrt(normb_a)));
		}
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

}
