package sci2s.mrfingerprint;


public interface PartialScoreMCC extends PartialScore {
	
	// For Spark: get the score from a single PS (when the aggregation is finished)
	public double computeScore(int inputsize);
}
