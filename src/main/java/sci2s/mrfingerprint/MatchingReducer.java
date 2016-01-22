package sci2s.mrfingerprint;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MatchingReducer extends Reducer<PartialScoreKey, GenericPSWrapper, DoubleWritable, Text> {

	protected Map<?,?> infomap;
	protected PartialScore pssample;
	
	protected DoubleWritable outkey = new DoubleWritable();
	protected Text outvalue = new Text();
	
	@Override
	public void setup(Context context) {

		try {
			pssample = (PartialScore) Util.getClassFromProperty(context, "PartialScore").newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		infomap = pssample.loadReducerInfoFile(context.getConfiguration());
	}

	@Override
	public void reduce(PartialScoreKey key, Iterable<GenericPSWrapper> values, Context context) 
			throws IOException, InterruptedException {

		// Aggregate the PartialScore set
		double score = pssample.aggregateG(key, values, infomap);
		
		outkey.set(score);
		outvalue.set(key.toString());

		// Insert the score + fpid into the output
		if(score > 0.0)
			context.write(outkey, outvalue);
	}
}