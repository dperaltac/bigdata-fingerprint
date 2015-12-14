package sci2s.resultsanalyzer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ResultsAnalyzerReducer extends Reducer<Text, ScorePair, IntWritable, NullWritable> {
	
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

		if(fc.genuine(bestpair.getFpid(), key.toString()))
			context.write(new IntWritable(1), null);
	}
}
