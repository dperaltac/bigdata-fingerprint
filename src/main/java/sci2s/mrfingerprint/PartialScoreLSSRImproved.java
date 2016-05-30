package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;


public class PartialScoreLSSRImproved implements PartialScore {

	protected Map<Integer, Minutia> tls;
	protected TopN<LocalMatch> lmatches;

	static protected class LocalMatch implements Comparable<LocalMatch>, Writable {

		public int b1;
		public int b2;
		public double sl;

		public LocalMatch() {
			b1 = 0;
			b2 = 0;
			sl = 0.0;
		}

		public LocalMatch(LocalMatch o) {
			b1 = o.b1;
			b2 = o.b2;
			sl = o.sl;
		}

		public LocalMatch(int b1, int b2, double sl) {
			this.b1 = b1;
			this.b2 = b2;
			this.sl = sl;
		}

		public int compareTo(LocalMatch arg0) {
			int res = Double.compare(sl, arg0.sl);

			if(res == 0) {
				res = Integer.compare(b1,arg0.b1);

				if(res == 0)
					return Integer.compare(b2,arg0.b2);
			}

			return res;
		}

		public void readFields(DataInput arg0) throws IOException {
			b1 = arg0.readInt();
			b2 = arg0.readInt();
			sl = arg0.readDouble();
		}

		public void write(DataOutput arg0) throws IOException {
			arg0.writeInt(b1);
			arg0.writeInt(b2);
			arg0.writeDouble(sl);
		}
	}

	static protected class LocalMatchArray extends ArrayWritable {
		public LocalMatchArray() {
			super(LocalMatch.class);
		}

		public LocalMatchArray(LocalMatch [] values) {
			super(LocalMatch.class, values);
		}
	}

	public static final int NREL = 5;
	public static final double WR = 0.5; //!< Weight parameter in Relaxation computation
	public static final double MUP1 = 5; //!< Sigmoid parameter 1 in the computation of d_1 relaxation
	public static final double TAUP1 = -1.6; //!< Sigmoid parameter 2 in the computation of d_1 relaxation
	public static final double MUP2 = 0.261799387799; //!< Sigmoid parameter 1 in the computation of d_2 relaxation
	public static final double TAUP2 = -30; //!< Sigmoid parameter 2 in the computation of d_2 relaxation
	public static final double MUP3 = 0.261799387799; //!< Sigmoid parameter 1 in the computation of d_3 relaxation
	public static final double TAUP3 = -30; //!< Sigmoid parameter 2 in the computation of d_3 relaxation

	public static final int MAX_LMATCHES = 250;

	public PartialScoreLSSRImproved() {

		tls = null;
		lmatches = null;
	}

	public PartialScoreLSSRImproved(PartialScoreLSSRImproved o) {

		tls = new HashMap<Integer, Minutia>(o.tls);
		lmatches = new TopN<LocalMatch>(o.lmatches);
	}

	// Parameter constructor. Performs the partialAggregate operation.
//	public PartialScoreLSSRImproved(Iterable<GenericPSWrapper> values, int nr) {
//
//		PartialScoreLSSRImproved psc;
//		lmatches = new TopN<LocalMatch>(nr);
//
//		for(GenericPSWrapper ps : values) {
//			psc = (PartialScoreLSSRImproved) ps.get();
//
//			lmatches.addAll(psc.lmatches);
//			tls.putAll(psc.tls);
//		}
//	}

	// Parameter constructor. Performs the partialAggregate operation.
	public PartialScoreLSSRImproved(Iterable<GenericPSWrapper> values) {

		PartialScoreLSSRImproved psc;

		lmatches = new TopN<LocalMatch>(MAX_LMATCHES);
		tls = new HashMap<Integer, Minutia>();

		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSSRImproved) ps.get();

			if(psc.lmatches.getMax() < lmatches.getMax())
				lmatches.setMax(psc.lmatches.getMax());

			lmatches.addAll(psc.lmatches);
			tls.putAll(psc.tls);
		}
	}

	// Parameter constructor. Performs the partialAggregate operation.
	public static PartialScoreLSSRImproved partialAggregateG(Iterable<PartialScoreLSSRImproved> values) {

		PartialScoreLSSRImproved result = new PartialScoreLSSRImproved();
		
		result.lmatches = new TopN<LocalMatch>(MAX_LMATCHES);
		result.tls = new HashMap<Integer, Minutia>();

		for(PartialScoreLSSRImproved psc : values) {

			if(psc.lmatches.getMax() < result.lmatches.getMax())
				result.lmatches.setMax(psc.lmatches.getMax());

			result.lmatches.addAll(psc.lmatches);
			result.tls.putAll(psc.tls);
		}
		
		return result;
	}

	public PartialScoreLSSRImproved (LocalStructure ls, LocalStructure[] als) {

		double sl;

		// If the cylinder is not valid, no partial score is computed
		if(!((LocalStructureCylinder) ls).isValid()) {

			tls = null;
			lmatches = null;

			return;
		}

		tls = new HashMap<Integer, Minutia>(1);
		tls.put(ls.getLSid(), ((LocalStructureCylinder) ls).getMinutia());

		// als.length is an upper bound of the real nr
		lmatches = new TopN<LocalMatch>(als.length / 2);

		for(int i = 0; i < als.length; i++) {

			LocalStructureCylinder ilsc = (LocalStructureCylinder) als[i];

			// If the cylinder is not valid, no partial score is computed
			if(ilsc.isValid()) {
				try {
					sl = ls.similarity(ilsc);

					if(sl > 0.0) 
						lmatches.add(new LocalMatch(ls.getLSid(), ilsc.getLSid(), sl));

				} catch (LSException e) {
					System.err.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public String toString() {
		String res = super.toString();

		res += tls.size();

		for(Minutia m : tls.values())
			res += ";" + m.toString();


		for(LocalMatch tmp : lmatches)
			res += ";" + "(" + tmp.b1 + "," + tmp.b2 + "," + tmp.sl + ")";

		return res;
	}

	public void readFields(DataInput in) throws IOException {

		MinutiaArray auxaw = new MinutiaArray();

		// Read the template local structures
		auxaw.readFields(in);
		Writable [] writables = auxaw.get();
		tls = new HashMap<Integer, Minutia>(writables.length);

		for(Writable w : writables) {
			Minutia m = (Minutia) w;
			tls.put(m.getIndex(), m);
		}

		// Read nr
		int nr = in.readInt();
		lmatches = new TopN<LocalMatch>(nr);

		// Read the local matches
		ArrayWritable auxaw2 = new ArrayWritable(LocalMatch.class);
		auxaw2.readFields(in);
		
		for(Writable w : auxaw2.get())
			lmatches.add((LocalMatch) w);
	}

	public void write(DataOutput out) throws IOException {

		// Write the template local structures
		MinutiaArray auxaw = new MinutiaArray(tls.values().toArray(new Minutia[0]));
		auxaw.write(out);

		// Write nr
		out.writeInt(lmatches.getMax());

		// Write the local matches
		ArrayWritable auxaw2 = new LocalMatchArray(lmatches.toArray(new LocalMatch[0]));
		auxaw2.write(out);
	}

	public static double rho(Minutia t_a, Minutia t_b, Minutia k_a, Minutia k_b) {

		double d1, d2, d3;

		d1 = Math.abs(t_a.getDistance(k_a.getX(),k_a.getY()) - t_b.getDistance(k_b.getX(),k_b.getY()));
		d2 = Math.abs(Util.dFiMCC(Util.dFiMCC(t_a.getrT(),k_a.getrT()),Util.dFiMCC(t_b.getrT(),k_b.getrT())));
		d3 = Math.abs(Util.dFiMCC(dR(t_a,k_a),dR(t_b,k_b)));

		return Util.psi(d1,MUP1,TAUP1) * Util.psi(d2,MUP2,TAUP2) * Util.psi(d3,MUP3,TAUP3);
	}

	public static double dR(Minutia m1, Minutia m2)
	{
		return Util.dFiMCC(m1.getrT(), Math.atan2(m1.getY()-m2.getY(), m2.getX()-m1.getX()));
	}


	public double aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		PartialScoreLSSRImproved bestps = new PartialScoreLSSRImproved(values);

		return bestps.computeScore(key.getFpidInput().toString(), infomap);
	}

	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {
		return new PartialScoreLSSRImproved(values);
	}

	// Saves the minutiae of the input local structures
	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {

		String name = conf.get(Util.INFOFILENAMEPROPERTY, Util.INFOFILEDEFAULTNAME);

		MapFile.Writer lsmapfile = Util.createMapFileWriter(conf, name, Text.class, MinutiaArray.class);

		Arrays.sort(inputls, new Comparator<LocalStructure[]>() {
			public int compare(LocalStructure [] als1, LocalStructure [] als2) {
				return als1[0].getFpid().compareTo(als2[0].getFpid());
			}
		});

		Text fpid = new Text();

		for(LocalStructure [] ails : inputls) {
			fpid.set(ails[0].getFpid());

			Minutia [] ma = new Minutia[ails.length];

			try {
				for(int i = 0; i < ails.length; i++)
					ma[i] = ((LocalStructureCylinder) ails[i]).getMinutia();

				lsmapfile.append(fpid, new MinutiaArray(ma));
			} catch (IOException e) {
				System.err.println("LocalStructure.saveLSMapFile: unable to save fingerprint "
						+ fpid.toString() + " in MapFile " + name + ": " + e.getMessage());
				e.printStackTrace();
			}
		}

		IOUtils.closeStream(lsmapfile);

		return;
	}

	public static Map<String, Minutia[]> loadInfoFile(Configuration conf) {

		Map<String, Minutia[]> infomap = new HashMap<String, Minutia[]>();

		String name = conf.get(Util.INFOFILENAMEPROPERTY, Util.INFOFILEDEFAULTNAME);
		MapFile.Reader lsmapfile = Util.createMapFileReader(conf, name);

		WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(lsmapfile.getKeyClass(), conf);

		MinutiaArray value = (MinutiaArray) ReflectionUtils.newInstance(lsmapfile.getValueClass(), conf);

		try {
			while(lsmapfile.next(key, value)) {
				infomap.put(key.toString(), Arrays.copyOf(value.get(), value.get().length, Minutia[].class));
			}
		} catch (Exception e) {
			System.err.println("PartialScoreLSSR.loadInfoFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}

		IOUtils.closeStream(lsmapfile);

		return infomap;
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

	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureCylinder.class);
	}

	public PartialScore computePartialScore(LocalStructure ls, LocalStructure[] als) {
		return new PartialScoreLSSRImproved(ls, als);
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		// Does nothing
		return null;
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public boolean isEmpty() {
		return (lmatches == null || tls == null || tls.isEmpty() || lmatches.isEmpty());
	}

	public static int computeNP(int n_A, int n_B)
	{
		return PartialScoreLSS.computeNP(n_A, n_B);
	}

	public static int computeNP(int n_A)
	{
		return PartialScoreLSS.computeNP(n_A);
	}

//	public PartialScore aggregateSinglePS(PartialScore ps) {
//
//		PartialScoreLSSR result = new PartialScoreLSSR(this);
//		PartialScoreLSSR psc = (PartialScoreLSSR) ps;
//
//		// Initialize member variables
//		result.lmatches.addAll(psc.lmatches);
//		result.tls.putAll(psc.tls);
//
//		return result;
//	}

	public PartialScore aggregateSinglePS(PartialScore ps) {
		PartialScoreLSSRImproved psc = (PartialScoreLSSRImproved) ps;

		// Initialize member variables
		lmatches.addAll(psc.lmatches);
		tls.putAll(psc.tls);

		return this;
	}

	public double computeScore(Minutia [] inputmin) {

		// Extract the best nr pairs (LSS consolidation)
		int nr = Math.min(Math.min(inputmin.length, tls.size()), lmatches.size());
		int np = computeNP(inputmin.length, tls.size());

		lmatches.truncate(nr);
		LocalMatch[] bestlm = lmatches.toArray(new LocalMatch[nr]);

		return consolidation(inputmin, bestlm, np);
	}

	public double computeScore(String input_fpid, Map<?, ?> infomap) {
		return computeScore((Minutia []) infomap.get(input_fpid));
	}

	protected double consolidation(Minutia [] inputmin, LocalMatch[] bestlm, int np)
	{
		int nr = bestlm.length;

		double [] lambdaT = new double[bestlm.length];
		double [] lambdaT1 = new double[lambdaT.length];

		// Concatenate all similarity values
		for(int i = 0; i < nr; i++)
			lambdaT[i] = bestlm[i].sl;

		final double LAMBDAWEIGHT = (1.0-WR)/(nr-1.0);

		double [][] rhotab = new double[nr][nr];

		double sum = 0.0;

		Arrays.sort(inputmin);

		for (int j=0; j<nr; j++)
			for (int k=0; k<nr; k++)
				if (k!=j)
					rhotab[j][k] = rho(
							tls.get(bestlm[j].b1),
							inputmin[bestlm[j].b2],
							tls.get(bestlm[k].b1),
							inputmin[bestlm[k].b2]);
				else
					rhotab[j][k] = 0;

		// Apply relaxation iterations
		for (int i=0; i<NREL; i++)
		{
			double [] tmp = lambdaT;
			lambdaT = lambdaT1;
			lambdaT1 = tmp;

			for (int j=0; j<nr; j++)
			{
				sum = 0.0;
				for (int k=0; k<nr; k++)
					sum += rhotab[j][k] * lambdaT1[k];

				lambdaT[j] = WR*lambdaT1[j] + LAMBDAWEIGHT*sum;
			}
		}

		double [] efficiency = new double[nr];

		for (int i=0; i<nr; i++)
			efficiency[i] = lambdaT[i] / bestlm[i].sl;

		Integer [] besteffidx = Util.sortIndexes(efficiency);

		sum = 0.0;

		for (int i=0; i<np; i++)
			sum += lambdaT[besteffidx[nr-i-1]];

		return sum/np;
	}
}