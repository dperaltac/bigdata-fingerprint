package sci2s.resultsanalyzer;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ResultsAnalyzerReducerDetailed extends Reducer<Text, ScorePair, Text, Text> {

	protected FingerprintComparison fc;
	//	private static final Logger log = LoggerFactory.getLogger(ResultsAnalyzer.class);

	@Override
	public void setup(Context context) {

		fc = new FingerprintComparison(context.getConfiguration().get("database"));
	}

	@Override
	public void reduce(Text key, Iterable<ScorePair> values, Context context) 
			throws IOException, InterruptedException {

		ScorePair bestpair = new ScorePair("none", -1);
		System.out.println("Reduce for " + key.toString());

		for(ScorePair sp : values) {

			//			log.info(bestpair.getFpid() + ' ' + bestpair.getScore() + ' ' + key.toString());
//			System.out.println("\tEvaluating (" + sp.getFpid() + ';' + sp.getScore() + ")");

			if(bestpair.getScore() < sp.getScore()) {

//				System.out.println("New best score for " + key.toString() + ": (" +
//						bestpair.getFpid() + ';' + bestpair.getScore() + ") --> (" +
//						sp.getFpid() + ';' + sp.getScore() + ")");
				
				bestpair = new ScorePair(sp);
			}


			//			if(fc.genuine(bestpair.getFpid(), key.toString())) {

			String result = sp.getFpid() + "\t" + sp.getScore() + "\t" + fc.genuine(sp.getFpid(), key.toString());
			context.write(key, new Text(result));
			//			}
		}
	}
}
