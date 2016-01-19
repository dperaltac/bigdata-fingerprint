package sci2s.mrfingerprint;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer;

public class MatchingCombiner extends Reducer<PartialScoreKey, GenericPSWrapper, PartialScoreKey, GenericPSWrapper> {
	
	protected Map<?,?> infomap;
	protected PartialScore pssample;
	
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
		infomap = pssample.loadCombinerInfoFile(context.getConfiguration());
	}

	@Override
	public void reduce(PartialScoreKey key, Iterable<GenericPSWrapper> values, Context context) 
			throws IOException, InterruptedException {
	    
	    context.progress();
	
		// Aggregate the PartialScore set.	 
		PartialScore ps = pssample.partialAggregateG(key, values, infomap);

		// Insert the score + fpid into the output
		if(ps != null && !ps.isEmpty())
			context.write(key, new GenericPSWrapper(ps));
	}
}