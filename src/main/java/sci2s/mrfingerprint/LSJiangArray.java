package sci2s.mrfingerprint;

import org.apache.hadoop.io.ArrayWritable;

//TODO maybe include a size field and function
public class LSJiangArray extends ArrayWritable {

	public LSJiangArray() {
		super(LocalStructureJiang.class);
	}

	public LSJiangArray(String[] arg0) {
		super(arg0);
	}

	public LSJiangArray(LocalStructureJiang[] values) {
		super(LocalStructureJiang.class, values);
	}

	public LSJiangArray(LocalStructure[] values) {
		super(LocalStructureJiang.class, values);
	}

	public LSJiangArray(LSJiangArray o) {
		super(LocalStructureJiang.class, o.get());
	}

}
