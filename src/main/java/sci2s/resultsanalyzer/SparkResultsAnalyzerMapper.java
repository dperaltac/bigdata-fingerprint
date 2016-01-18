package sci2s.resultsanalyzer;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SparkResultsAnalyzerMapper extends
		Mapper<LongWritable, Text, Text, ScorePair> {

	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	
		String[] parts = value.toString().replaceAll("[()]","").split(",");
	
		// The key is the input fingerprint
		// The value is the template fingerprint along with the score	
		context.write(new Text(parts[1]),
				new ScorePair(parts[0].substring(parts[0].indexOf(';')+1), Double.parseDouble(parts[2])));
	}
}
