package sci2s.mrfingerprint;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;


public class GenericPSWrapper extends GenericWritable {

	@SuppressWarnings("rawtypes")
	private static Class[] CLASSES = {
		PartialScoreJiang.class,
		PartialScoreJiangLocal.class,
		PartialScoreLSS.class,
		PartialScoreLSSR.class,
		PartialScoreLSSImproved.class,
		PartialScoreLSSThreshold.class,
		PartialScoreLSSRImproved.class
	};
	
	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

    //this empty initialize is required by Hadoop
    public GenericPSWrapper() {
    }

    public GenericPSWrapper(Writable instance) {
        set(instance);
    }

    @Override
    public String toString() {
        return "GenericPSWrapper [getTypes()=" + Arrays.toString(getTypes()) + "]";
    }
	
	public static List<PartialScore> unwrap(Iterable<GenericPSWrapper> ipsw) {
			
		ArrayList<PartialScore> apsw = new ArrayList<PartialScore>();
		
		for(GenericPSWrapper elem : ipsw)
			apsw.add((PartialScore) elem.get());
		
		return apsw;
	}
}
