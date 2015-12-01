package sci2s.resultsanalizer;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;


public class ResultsAnalyzerCombiner extends Reducer<Text, ScorePair, Text, ScorePair> {
	
	protected FingerprintComparison fc;
	private static final Logger log = LoggerFactory.getLogger(ResultsAnalyzerReducerDetailed.class);
	
	@Override
	public void setup(Context context) {
		fc = new FingerprintComparison(context.getConfiguration().get("database"));
	}

	@Override
	public void reduce(Text key, Iterable<ScorePair> values, Context context) 
			throws IOException, InterruptedException {
		
		ScorePair bestpair = new ScorePair("", -1);

		for(ScorePair sp : values) {
			if(bestpair.compareTo(sp) < 0)
				bestpair = sp;
		}
		
		context.write(key, bestpair);
	}
}
