package sci2s.resultsanalizer;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ResultsAnalyzerMapper extends
		Mapper<Text, Text, Text, ScorePair> {

	private ScorePair sp = new ScorePair();

	@Override
	  public void map(Text key, Text value, Context context)
	      throws IOException, InterruptedException {
	
		String[] parts = value.toString().split(";");
	
		sp.set(parts[0], Double.parseDouble(key.toString()));
		
		context.write(new Text(parts[1]), sp);
	}
}
