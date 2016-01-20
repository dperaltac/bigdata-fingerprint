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
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class PartialScoreLSS implements PartialScoreMCC {

	protected double [] bestsimilarities;
	protected int templatesize;

	public static final double MUP = 20; //!< Sigmoid parameter 1 in the computation of n_P
	public static final double TAUP = 0.4; //!< Sigmoid parameter 2 in the computation of n_P
	public static final int MINNP = 4; //!< Minimum number of minutiae in the computation of n_P
	public static final int MAXNP = 12; //!< Maximum number of minutiae in the computation of n_P

	public PartialScoreLSS() {

		bestsimilarities = new double[0];
		templatesize = 0;

	}

	public PartialScoreLSS(PartialScoreLSS o) {

		bestsimilarities = Arrays.copyOf(o.bestsimilarities, o.bestsimilarities.length);
		templatesize = o.templatesize;

	}

	public PartialScoreLSS(double [] bs, int ts) {

		bestsimilarities = Arrays.copyOf(bs, bs.length);
		templatesize = ts;
	}


	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreLSS(Iterable<GenericPSWrapper> values, int np) {

		TopN<Double> best = new TopN<Double>(np);
		PartialScoreLSS psc;

		templatesize = 0;

		// Aggregate all similarity values
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSS) ps.get();

			for(double sl : psc.bestsimilarities)
				if(sl > 0.0)
					best.add(sl);

			templatesize += psc.templatesize;
		}

		bestsimilarities = new double[best.size()];
		for(int i = 0; i < bestsimilarities.length; ++i)
			bestsimilarities[i] = best.poll();
	}

	public PartialScoreLSS(LocalStructure ls, LocalStructure [] als) {

		// If the cylinder is not valid, no partial score is computed
		if(!((LocalStructureCylinder) ls).isValid()) {

			bestsimilarities = new double[0];
			templatesize = 1;

			return;
		}

		TopN<Double> gamma = new TopN<Double>(computeNP(als.length));
		double sl;

		for(LocalStructure ils : als) {

			// If the cylinder is not valid, no partial score is computed
			if(((LocalStructureCylinder) ils).isValid()) {
				try {
					sl = ls.similarity(ils);

					if(sl > 0.0)
						gamma.add(sl);

				} catch (LSException e) {
					System.err.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}

		bestsimilarities = new double[gamma.size()];
		for(int i = 0; i < bestsimilarities.length; ++i)
			bestsimilarities[i] = gamma.poll();

		templatesize = 1;
	}

	@Override
	public String toString() {
		return bestsimilarities.toString();
	}

	public void readFields(DataInput in) throws IOException {

		templatesize = in.readInt();

		ArrayPrimitiveWritable auxaw = new ArrayPrimitiveWritable(bestsimilarities);
		auxaw.readFields(in);
		bestsimilarities = (double[]) auxaw.get();
	}

	public void write(DataOutput out) throws IOException {

		out.writeInt(templatesize);

		ArrayPrimitiveWritable auxaw = new ArrayPrimitiveWritable(bestsimilarities);
		auxaw.write(out);
	}


	public static int computeNP(int n_A, int n_B)
	{
		return MINNP + (int)Math.round(Util.psi(Math.min(n_A,n_B), MUP, TAUP*(MAXNP-MINNP)));
	}

	public static int computeNP(int n_A)
	{
		return MINNP + (int)Math.round(Util.psi(n_A, MUP, TAUP*(MAXNP-MINNP)));
	}



	public double aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		int tam = 0;
		double sum = 0.0;
		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());
		TopN<Double> best = new TopN<Double>(computeNP(inputsize));

		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}

		// Concatenate all similarity values
		for(GenericPSWrapper ps : values) {

			PartialScoreLSS psc = (PartialScoreLSS) ps.get();

			for(double sl : psc.bestsimilarities)
				best.add(sl);

			tam += psc.templatesize;
		}

		int np = computeNP(inputsize, tam);

		for(int i = 0; i < np; i++)
			sum += best.poll();

		return sum/np;

	}


	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());

		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}

		return new PartialScoreLSS(values, computeNP(inputsize));
	}


	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {

		String name = conf.get(Util.INFOFILENAMEPROPERTY, Util.INFOFILEDEFAULTNAME);
		MapFile.Writer infofile = Util.createMapFileWriter(conf, name, Text.class, IntWritable.class);

		Arrays.sort(inputls, new Comparator<LocalStructure[]>() {
			public int compare(LocalStructure [] als1, LocalStructure [] als2) {
				return als1[0].fpid.compareTo(als2[0].fpid);
			}
		});

		for(LocalStructure [] ails : inputls) {
			String fpid = ails[0].fpid;

			try {
				infofile.append(new Text(fpid), new IntWritable(ails.length));
			} catch (IOException e) {
				System.err.println("PartialScoreCylinder.saveInfoFile: unable to save fingerprint "
						+ fpid + " in MapFile " + name + ": " + e.getMessage());
				e.printStackTrace();
			}
		}

		IOUtils.closeStream(infofile);
	}

	public Map<String, Integer> loadInfoFile(Configuration conf) {

		String name = conf.get(Util.INFOFILENAMEPROPERTY, Util.INFOFILEDEFAULTNAME);
		MapFile.Reader infofile = Util.createMapFileReader(conf, name);

		Map<String, Integer> infomap = new HashMap<String,Integer>();

		Text key = new Text();
		IntWritable value = new IntWritable();

		try {
			while(infofile.next(key, value)) {
				infomap.put(key.toString(), value.get());
			}
		} catch (Exception e) {
			System.err.println("PartialScoreCylinder.loadInfoFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}

		IOUtils.closeStream(infofile);

		return infomap;
	}

	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureCylinder.class);
	}

	public PartialScore computePartialScore(LocalStructure ls, LocalStructure[] als) {
		return new PartialScoreLSS(ls, als);
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public boolean isEmpty() {
		return (bestsimilarities == null || bestsimilarities.length == 0 || bestsimilarities[0] <= 0);
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {

		PartialScoreLSS psc = (PartialScoreLSS) ps;
		PartialScoreLSS result = new PartialScoreLSS();

		final int MAX_SIMS = computeNP(250);

		if(psc.bestsimilarities.length + bestsimilarities.length > MAX_SIMS) {
			TopN<Double> topn = new TopN<Double>(ArrayUtils.toObject(bestsimilarities), MAX_SIMS);
			topn.addAll(ArrayUtils.toObject(psc.bestsimilarities));

			result.bestsimilarities = ArrayUtils.toPrimitive(topn.toArray(new Double[0]));
		}
		else if(psc.bestsimilarities.length + bestsimilarities.length > 0) {
			result.bestsimilarities = ArrayUtils.addAll(bestsimilarities, psc.bestsimilarities);				
		}
		else {
			result.bestsimilarities = new double[0];
		}

		result.templatesize = psc.templatesize + templatesize;

		return result;
	}

	public void aggregateSingleValue(double value) {

		int minpos = Util.minPosition(bestsimilarities);

		if(bestsimilarities[minpos] < value)
			bestsimilarities[minpos] = value;
	}

	public double computeScore(int inputsize) {

		int np = computeNP(inputsize, templatesize);
		double sum = 0.0;

		int np2 = Math.min(np, bestsimilarities.length);

		if(np2 == 0)
			return 0;

		for(int i = 0; i < np2; i++)
			sum += bestsimilarities[i];

		return sum/np;

	}

	public double computeScore(String input_fpid, Map<?, ?> infomap) {

		Integer inputsize = (Integer) infomap.get(input_fpid);
		return computeScore(inputsize);
	}
}
