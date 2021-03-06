package sci2s.resultsanalyzer;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ResultsAnalyzerCombiner extends Reducer<Text, ScorePair, Text, ScorePair> {
	
	protected FingerprintComparison fc;
	
	@Override
	public void setup(Context context) {
		fc = new FingerprintComparison(context.getConfiguration().get("database"));
	}

	@Override
	public void reduce(Text key, Iterable<ScorePair> values, Context context) 
			throws IOException, InterruptedException {
		
		ScorePair bestpair = new ScorePair("", -1);

		for(ScorePair sp : values) {
			if(bestpair.getScore() < sp.getScore()) {
				bestpair = new ScorePair(sp);
			}
		}
		
		context.write(key, bestpair);
	}
}
