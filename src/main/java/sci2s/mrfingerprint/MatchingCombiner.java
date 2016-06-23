package sci2s.mrfingerprint;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class MatchingCombiner extends Reducer<PartialScoreKey, GenericPSWrapper, PartialScoreKey, GenericPSWrapper> {
	
//	protected Map<?,?> infomap;
	protected PartialScore pssample;
	
	static enum CombinerCountersEnum {
		TOTAL_COMBINER_MILLIS ,
		TOTAL_COMBINERTASK_MILLIS ,
		COMBINER_NUMBER ,
		COMBINERTASK_NUMBER }
	
	Counter counter_combiner_millis;
	Counter counter_combinertask_millis;
	Counter counter_combiner_number;
	long combinertask_init_time;
	
	@Override
	public void setup(Context context) {
		
		combinertask_init_time = System.currentTimeMillis();

		try {
			pssample = (PartialScore) Util.getClassFromProperty(context, "PartialScore").newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		infomap = pssample.loadCombinerInfoFile(context.getConfiguration());

		counter_combiner_millis = context.getCounter(CombinerCountersEnum.class.getName(),
				CombinerCountersEnum.TOTAL_COMBINER_MILLIS.toString());

		counter_combinertask_millis = context.getCounter(CombinerCountersEnum.class.getName(),
				CombinerCountersEnum.TOTAL_COMBINERTASK_MILLIS.toString());

		counter_combiner_number = context.getCounter(CombinerCountersEnum.class.getName(),
				CombinerCountersEnum.COMBINER_NUMBER.toString());

		Counter maptask_number = context.getCounter(CombinerCountersEnum.class.getName(),
				CombinerCountersEnum.COMBINERTASK_NUMBER.toString());
		maptask_number.increment(1);
	}

	@Override
	public void reduce(PartialScoreKey key, Iterable<GenericPSWrapper> values, Context context) 
			throws IOException, InterruptedException {

	    context.progress();
		
		long init_time = System.currentTimeMillis();
	
		// Aggregate the PartialScore set.	 
//		pssample.partialAggregateG(key, values, infomap);
		pssample.partialAggregateG(values);

		// Insert the score + fpid into the output
		if(pssample != null && !pssample.isEmpty())
			context.write(key, new GenericPSWrapper(pssample));

        counter_combiner_millis.increment(System.currentTimeMillis() - init_time);
        counter_combiner_number.increment(1);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		counter_combinertask_millis.increment(System.currentTimeMillis() - combinertask_init_time);
	}
}