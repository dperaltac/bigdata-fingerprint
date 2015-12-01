package sci2s.mrfingerprint;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MatchingMapper extends Mapper<Text, LocalStructure, PartialScoreKey, GenericPSWrapper> {

	protected LocalStructure[][] inputls;
	protected PartialScore pssample;

	protected PartialScoreKey psk = new PartialScoreKey();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

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
	}

	@Override
	public void map(Text key, LocalStructure value, Context context)
	throws IOException, InterruptedException {

		// De-serialize the LocalStructure in "value"
//		LocalStructure ls = (LocalStructure) value.get();
		LocalStructure ls = value;
		
		GenericPSWrapper gpsw = new GenericPSWrapper();
		
		// Find the maximum similarity between that LS and all those from the input fingerprint (partial score)
		for(LocalStructure[] ilsarray : inputls) {

			// Compute the partial score for this input fingerprint
			PartialScore ps = pssample.computePartialScore(ls, ilsarray);

			if(!ps.isEmpty())
			{
				gpsw.set(ps);

				// Output key
				psk.set(ls.getFpid(), ilsarray[0].getFpid());
				context.write(psk, gpsw);
			}
		}
	}

}
