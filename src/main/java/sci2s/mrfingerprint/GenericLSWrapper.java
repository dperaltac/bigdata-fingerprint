package sci2s.mrfingerprint;
import java.util.Arrays;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;


public class GenericLSWrapper extends GenericWritable {
	
	@SuppressWarnings("rawtypes")
	private static Class[] CLASSES = {
		LocalStructureJiang.class,
		LocalStructureCylinder.class
	};
	
	@SuppressWarnings("unchecked")
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

    //this empty initialize is required by Hadoop
    public GenericLSWrapper() {
    }

    public GenericLSWrapper(Writable instance) {
        set(instance);
    }

    @Override
    public String toString() {
        return "GenericLSWrapper [getTypes()=" + Arrays.toString(getTypes()) + "]";
    }
}
