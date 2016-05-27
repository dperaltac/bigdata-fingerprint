package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;

public class PartialScoreLSSImproved implements PartialScore {

	protected double [] bestsimilarities;

	public static final int AVGNP = 5; //!< My arbitrary value for n_P

	public PartialScoreLSSImproved() {

		bestsimilarities = new double[0];

	}

	public PartialScoreLSSImproved(PartialScoreLSSImproved o) {

		bestsimilarities = Arrays.copyOf(o.bestsimilarities, o.bestsimilarities.length);

	}

	public PartialScoreLSSImproved(double [] bs, int ts) {

		bestsimilarities = Arrays.copyOf(bs, bs.length);
	}


	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreLSSImproved(Iterable<GenericPSWrapper> values) {

		TopN<Double> best = new TopN<Double>(AVGNP);
		PartialScoreLSSImproved psc;

		// Aggregate all similarity values
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSSImproved) ps.get();

			for(double sl : psc.bestsimilarities)
				if(sl > 0.0)
					best.add(sl);
		}

		bestsimilarities = new double[best.size()];
		for(int i = 0; i < bestsimilarities.length; ++i)
			bestsimilarities[i] = best.poll();
	}

	public PartialScoreLSSImproved(LocalStructure ls, LocalStructure [] als) {

		// If the cylinder is not valid, no partial score is computed
		if(!((LocalStructureCylinder) ls).isValid()) {

			bestsimilarities = new double[0];

			return;
		}

		TopN<Double> gamma = new TopN<Double>(AVGNP);
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
	}

	@Override
	public String toString() {
		return bestsimilarities.toString();
	}

	public void readFields(DataInput in) throws IOException {

		ArrayPrimitiveWritable auxaw = new ArrayPrimitiveWritable(bestsimilarities);
		auxaw.readFields(in);
		bestsimilarities = (double[]) auxaw.get();
	}

	public void write(DataOutput out) throws IOException {

		ArrayPrimitiveWritable auxaw = new ArrayPrimitiveWritable(bestsimilarities);
		auxaw.write(out);
	}



	public double aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		double sum = 0.0;
		TopN<Double> best = new TopN<Double>(AVGNP);

		// Concatenate all similarity values
		for(GenericPSWrapper ps : values) {

			PartialScoreLSSImproved psc = (PartialScoreLSSImproved) ps.get();

			// TODO : can be optimized?
			for(double sl : psc.bestsimilarities)
				best.add(sl);
		}

		for(Double b : best)
			sum += b;

		return sum/AVGNP;

	}


	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		return new PartialScoreLSSImproved(values);
	}


	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {

	}

	public Map<String, Integer> loadInfoFile(Configuration conf) {

		return null;
	}

	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureCylinder.class);
	}

	public PartialScore computePartialScore(LocalStructure ls, LocalStructure[] als) {
		return new PartialScoreLSSImproved(ls, als);
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

		PartialScoreLSSImproved psc = (PartialScoreLSSImproved) ps;

		if(psc.bestsimilarities.length + bestsimilarities.length > AVGNP) {
			TopN<Double> topn = new TopN<Double>(ArrayUtils.toObject(bestsimilarities), AVGNP);
			topn.addAll(ArrayUtils.toObject(psc.bestsimilarities));

			bestsimilarities = ArrayUtils.toPrimitive(topn.toArray(new Double[0]));
		}
		else if(psc.bestsimilarities.length + bestsimilarities.length > 0) {
			bestsimilarities = ArrayUtils.addAll(bestsimilarities, psc.bestsimilarities);				
		}
		else {
			bestsimilarities = new double[0];
		}

		return this;
	}

	public void aggregateSingleValue(double value) {

		int minpos = Util.minPosition(bestsimilarities);

		if(bestsimilarities[minpos] < value)
			bestsimilarities[minpos] = value;
	}

	public double computeScore() {

		if(bestsimilarities.length == 0)
			return 0;

		double sum = 0.0;

		int np2 = Math.min(AVGNP, bestsimilarities.length);

		for(int i = 0; i < np2; i++)
			sum += bestsimilarities[i];

		return sum/AVGNP;

	}

	public double computeScore(String input_fpid, Map<?, ?> infomap) {

		return computeScore();
	}
}
