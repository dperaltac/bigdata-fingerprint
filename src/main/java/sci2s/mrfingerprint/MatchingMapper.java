package sci2s.mrfingerprint;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

public class MatchingMapper extends Mapper<Text, LocalStructure, PartialScoreKey, GenericPSWrapper> {

	protected LocalStructure[][] inputls;
	protected PartialScore pssample;

	protected PartialScoreKey psk = new PartialScoreKey();
	
	static enum MapCountersEnum { TOTAL_MAP_MILLIS , TOTAL_MAPTASK_MILLIS , MAPTASK_NUMBER }
	
	Counter counter_map_millis;
	Counter counter_maptask_millis;
	long maptask_init_time;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		maptask_init_time = System.currentTimeMillis();

		inputls = LocalStructure.loadLSMapFile(context.getConfiguration());

		try {
			pssample = (PartialScore) Util.getClassFromProperty(context, "PartialScore").newInstance();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		counter_map_millis = context.getCounter(MapCountersEnum.class.getName(),
				MapCountersEnum.TOTAL_MAP_MILLIS.toString());

		counter_maptask_millis = context.getCounter(MapCountersEnum.class.getName(),
				MapCountersEnum.TOTAL_MAPTASK_MILLIS.toString());

		Counter maptask_number = context.getCounter(MapCountersEnum.class.getName(),
				MapCountersEnum.MAPTASK_NUMBER.toString());
		maptask_number.increment(1);
	}

	@Override
	public void map(Text key, LocalStructure value, Context context)
	throws IOException, InterruptedException {
		
		long init_time = System.currentTimeMillis();

		// De-serialize the LocalStructure in "value"
		LocalStructure ls = value;
    	
		GenericPSWrapper gpsw = new GenericPSWrapper();
		PartialScore ps = pssample.clone();
		
		// Find the maximum similarity between that LS and all those from the input fingerprint (partial score)
		for(LocalStructure[] ilsarray : inputls) {

			// Compute the partial score for this input fingerprint
			ps.computePartialScore(ls, ilsarray);

			if(!ps.isEmpty())
			{
				gpsw.set(ps);

				// Output key
				psk.set(ls.getFpid(), ilsarray[0].getFpid());
				context.write(psk, gpsw);
			}
		}

        counter_map_millis.increment(System.currentTimeMillis() - init_time);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);

		counter_maptask_millis.increment(System.currentTimeMillis() - maptask_init_time);
	}

}
