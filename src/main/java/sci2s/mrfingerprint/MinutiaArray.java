package sci2s.mrfingerprint;

import org.apache.hadoop.io.ArrayWritable;

//TODO maybe include a size field and function
public class MinutiaArray extends ArrayWritable {

	public MinutiaArray() {
		super(Minutia.class);
	}

	public MinutiaArray(String[] arg0) {
		super(arg0);
	}

	public MinutiaArray(Minutia[] values) {
		super(Minutia.class, values);
	}

	public MinutiaArray(MinutiaArray o) {
		super(Minutia.class, o.get());
	}

}
