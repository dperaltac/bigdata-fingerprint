package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class PartialScoreLSSThreshold implements PartialScore {

	protected float avgsim;
	protected int num;

	public static final float LS_THRESHOLD = 0.5f; //!< Threshold for local similarities

	public PartialScoreLSSThreshold() {

		avgsim = 0;
		num = 0;

	}

	public PartialScoreLSSThreshold(PartialScoreLSSThreshold o) {

		avgsim = 0;
		num = 0;

	}


	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreLSSThreshold(Iterable<GenericPSWrapper> values) {

		PartialScoreLSSThreshold psc;

		// Aggregate all similarity values
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSSThreshold) ps.get();
			
			avgsim += psc.avgsim;
			num += psc.num;
		}
	}

	public PartialScoreLSSThreshold(LocalStructure ls, LocalStructure [] als) {

		float sl;

		for(LocalStructure ils : als) {

			try {
				sl = ls.similarity(ils);

				if(sl > LS_THRESHOLD) {
					avgsim += sl;
					num++;
				}

			} catch (LSException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	@Override
	public String toString() {
		return "" + avgsim + "," + num;
	}

	public void readFields(DataInput in) throws IOException {
		
		avgsim = in.readFloat();
		num = in.readInt();
	}

	public void write(DataOutput out) throws IOException {

		out.writeFloat(avgsim);
		out.writeInt(num);
	}



	public float aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		return (new PartialScoreLSSThreshold(values).computeScore());
	}


	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		return new PartialScoreLSSThreshold(values);
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
		return new PartialScoreLSSThreshold(ls, als);
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public boolean isEmpty() {
		return (avgsim == 0 || num == 0);
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {

		PartialScoreLSSThreshold psc = (PartialScoreLSSThreshold) ps;
		avgsim += psc.avgsim;
		num += psc.num;

		return this;
	}

	public void aggregateSingleValue(float value) {

		avgsim += value;
		num++;
	}

	public float computeScore() {
		return avgsim / num;
	}

	public float computeScore(String input_fpid, Map<?, ?> infomap) {

		return computeScore();
	}
}
