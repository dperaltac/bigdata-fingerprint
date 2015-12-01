package sci2s.mrfingerprint;

import org.apache.hadoop.io.ArrayWritable;

// TODO maybe include a size field and function
public class LSCylinderArray extends ArrayWritable {

	public LSCylinderArray() {
		super(LocalStructureCylinder.class);
	}

	public LSCylinderArray(String[] arg0) {
		super(arg0);
	}

	public LSCylinderArray(LocalStructureCylinder[] values) {
		super(LocalStructureCylinder.class, values);
	}

	public LSCylinderArray(LocalStructure[] values) {
		super(LocalStructureCylinder.class, values);
	}

	public LSCylinderArray(LSCylinderArray o) {
		super(LocalStructureCylinder.class, o.get());
	}

}
