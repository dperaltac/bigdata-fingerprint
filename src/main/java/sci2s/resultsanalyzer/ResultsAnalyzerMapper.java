package sci2s.resultsanalyzer;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ResultsAnalyzerMapper extends
		Mapper<Text, Text, Text, ScorePair> {

	@Override
	  public void map(Text key, Text value, Context context)
	      throws IOException, InterruptedException {
	
		String[] parts = value.toString().split(";");
	
		// The key is the input fingerprint
		// The value is the template fingerprint along with the score	
		context.write(new Text(parts[1]), new ScorePair(parts[0], Double.parseDouble(key.toString())));
	}
}
