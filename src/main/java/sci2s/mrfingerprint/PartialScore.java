package sci2s.mrfingerprint;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;


public interface PartialScore extends Writable {

	public PartialScore computePartialScore(LocalStructure ls, LocalStructure[] als);

//	public double aggregate(PartialScoreKey key, Iterable<PartialScore> values, Map<?, ?> infomap);
	public float aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?, ?> infomap);

//	public PartialScore partialAggregate(PartialScoreKey key, Iterable<PartialScore> values, Map<?, ?> infomap);
	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?, ?> infomap);
	
	// For Spark: aggregation of two accumulators
	public PartialScore aggregateSinglePS(PartialScore ps);
	
	// For Spark: get the score from a single PS (when the aggregation is finished)
	public float computeScore(String input_fpid, Map<?, ?> infomap);
	
	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf);

	public Map<?,?> loadCombinerInfoFile(Configuration conf);
	
	public Map<?,?> loadReducerInfoFile(Configuration conf);
	
	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass);

	public boolean isEmpty();
}
