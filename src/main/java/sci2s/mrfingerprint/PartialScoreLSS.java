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

public class PartialScoreLSS implements PartialScore {

	protected float [] bestsimilarities;
	protected int templatesize;

	public static final float MUP = 20; //!< Sigmoid parameter 1 in the computation of n_P
	public static final float TAUP = 0.4f; //!< Sigmoid parameter 2 in the computation of n_P
	public static final int MINNP = 4; //!< Minimum number of minutiae in the computation of n_P
	public static final int MAXNP = 12; //!< Maximum number of minutiae in the computation of n_P

	public PartialScoreLSS() {

		bestsimilarities = new float[0];
		templatesize = 0;

	}

	public PartialScoreLSS(PartialScoreLSS o) {

		bestsimilarities = Arrays.copyOf(o.bestsimilarities, o.bestsimilarities.length);
		templatesize = o.templatesize;

	}

	public PartialScoreLSS(float [] bs, int ts) {

		bestsimilarities = Arrays.copyOf(bs, bs.length);
		templatesize = ts;
	}

	public PartialScoreLSS(LocalStructure ls, LocalStructure [] als) {
		
		computePartialScore(ls, als);
	}
	
	@Override
	public PartialScoreLSS clone() {
		PartialScoreLSS ps = new PartialScoreLSS(this);
		return ps;
	}

	@Override
	public String toString() {
		return bestsimilarities.toString();
	}

	public void readFields(DataInput in) throws IOException {

		templatesize = in.readInt();

		ArrayPrimitiveWritable auxaw = new ArrayPrimitiveWritable(bestsimilarities);
		auxaw.readFields(in);
		bestsimilarities = (float[]) auxaw.get();
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



	public float aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		int tam = 0;
		float sum = 0.0f;
		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());
		TopN<Float> best = new TopN<Float>(computeNP(inputsize));

		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}

		// Concatenate all similarity values
		for(GenericPSWrapper ps : values) {

			PartialScoreLSS psc = (PartialScoreLSS) ps.get();

			for(float sl : psc.bestsimilarities)
				best.add(sl);

			tam += psc.templatesize;
		}

		int np = computeNP(inputsize, tam);

		for(int i = 0; i < np; i++)
			sum += best.poll();

		return sum/np;

	}
	
	public void partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());

		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}
		
		partialAggregateG(values, computeNP(inputsize));
	}


	public void partialAggregateG(Iterable<GenericPSWrapper> values, int np) {


		TopN<Float> best = new TopN<Float>(np);
		PartialScoreLSS psc;

		templatesize = 0;

		// Aggregate all similarity values
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSS) ps.get();

			for(float sl : psc.bestsimilarities)
				if(sl > 0.0)
					best.add(sl);

			templatesize += psc.templatesize;
		}

		bestsimilarities = new float[best.size()];
		for(int i = 0; i < bestsimilarities.length; ++i)
			bestsimilarities[i] = best.poll();
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

	public void computePartialScore(LocalStructure ls, LocalStructure[] als) {

		TopN<Float> gamma = new TopN<Float>(computeNP(als.length));
		float sl;

		for(LocalStructure ils : als) {

			try {
				sl = ls.similarity(ils);

				if(sl > 0.0)
					gamma.add(sl);

			} catch (LSException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}

		bestsimilarities = new float[gamma.size()];
		for(int i = 0; i < bestsimilarities.length; ++i)
			bestsimilarities[i] = gamma.poll();

		templatesize = 1;
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

		final int MAX_SIMS = computeNP(250);

		if(psc.bestsimilarities.length + bestsimilarities.length > MAX_SIMS) {
			TopN<Float> topn = new TopN<Float>(ArrayUtils.toObject(bestsimilarities), MAX_SIMS);
			topn.addAll(ArrayUtils.toObject(psc.bestsimilarities));

			bestsimilarities = ArrayUtils.toPrimitive(topn.toArray(new Float[0]));
		}
		else if(psc.bestsimilarities.length + bestsimilarities.length > 0) {
			bestsimilarities = ArrayUtils.addAll(bestsimilarities, psc.bestsimilarities);				
		}
		else {
			bestsimilarities = new float[0];
		}

		templatesize = psc.templatesize + templatesize;

		return this;
	}

	public void aggregateSingleValue(float value) {

		int minpos = Util.minPosition(bestsimilarities);

		if(bestsimilarities[minpos] < value)
			bestsimilarities[minpos] = value;
	}

	public float computeScore(int inputsize) {

		int np = computeNP(inputsize, templatesize);
		float sum = 0.0f;

		int np2 = Math.min(np, bestsimilarities.length);

		if(np2 == 0)
			return 0;

		for(int i = 0; i < np2; i++)
			sum += bestsimilarities[i];

		return sum/np;

	}

	public float computeScore(String input_fpid, Map<?, ?> infomap) {

		Integer inputsize = (Integer) infomap.get(input_fpid);
		return computeScore(inputsize);
	}
}
