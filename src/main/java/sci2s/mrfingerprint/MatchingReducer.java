package sci2s.mrfingerprint;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class MatchingReducer extends Reducer<PartialScoreKey, GenericPSWrapper, DoubleWritable, Text> {

	protected Map<?,?> infomap;
	protected PartialScore pssample;
	
	protected DoubleWritable outkey = new DoubleWritable();
	protected Text outvalue = new Text();
	
	protected float threshold;
	
	static enum ReduceCountersEnum {
		TOTAL_REDUCE_MILLIS ,
		TOTAL_REDUCETASK_MILLIS ,
		REDUCE_NUMBER ,
		REDUCETASK_NUMBER }
	
	Counter counter_reduce_millis;
	Counter counter_reducetask_millis;
	Counter counter_reduce_number;
	long reducetask_init_time;
	
	@Override
	public void setup(Context context) {
		
		reducetask_init_time = System.currentTimeMillis();

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

		counter_reduce_millis = context.getCounter(ReduceCountersEnum.class.getName(),
				ReduceCountersEnum.TOTAL_REDUCE_MILLIS.toString());

		counter_reducetask_millis = context.getCounter(ReduceCountersEnum.class.getName(),
				ReduceCountersEnum.TOTAL_REDUCETASK_MILLIS.toString());

		counter_reduce_number = context.getCounter(ReduceCountersEnum.class.getName(),
				ReduceCountersEnum.REDUCE_NUMBER.toString());

		Counter maptask_number = context.getCounter(ReduceCountersEnum.class.getName(),
				ReduceCountersEnum.REDUCETASK_NUMBER.toString());
		maptask_number.increment(1);
		
		threshold = Util.getThreshold(context.getConfiguration());
	}

	@Override
	public void reduce(PartialScoreKey key, Iterable<GenericPSWrapper> values, Context context) 
			throws IOException, InterruptedException {
		
		long init_time = System.currentTimeMillis();

		// Aggregate the PartialScore set
		double score = pssample.aggregateG(key, values, infomap);

		// Insert the score + fpid into the output
		if(score > threshold) {
			outkey.set(score);
			outvalue.set(key.toString());
			context.write(outkey, outvalue);
		}

        counter_reduce_millis.increment(System.currentTimeMillis() - init_time);
        counter_reduce_number.increment(1);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		counter_reducetask_millis.increment(System.currentTimeMillis() - reducetask_init_time);
	}
}