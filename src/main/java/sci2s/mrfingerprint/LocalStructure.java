package sci2s.mrfingerprint;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;


public abstract class LocalStructure implements WritableComparable<LocalStructure>, java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected String fpid;
	protected int lsid;
	
	LocalStructure() {
		fpid = "";
		lsid = -1;
	}
	
	LocalStructure(LocalStructure ls) {
		this.fpid = ls.fpid;
		this.lsid = ls.lsid;
	}
	
	LocalStructure(String fpid, int lsid) {
		this.fpid = fpid;
		this.lsid = lsid;
	}
	
	public String getFpid() {
		return new String(fpid);
	}
	
	public int getLSid() {
		return lsid;
	}
	
	@Override
	public String toString() {
		return new String(fpid) + ";" + lsid;
	}
	
	public int compareTo(LocalStructure o) {
		int res = fpid.compareTo(o.fpid);
		
		if(res != 0)
			return res;
		
		return Integer.compare(lsid, o.lsid);
	}
	
	public static Class<? extends ArrayWritable> getArrayWritableClass() {
		return null;
	}
	
	public static String decodeFpid(String encoded) {
		
		// The fingerprint ID is the first token before the colon
		StringTokenizer st = new StringTokenizer(encoded, ":");
		return st.nextToken();
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends LocalStructure> T getInstanceFromText(String key, String value) {

		StringTokenizer st = new StringTokenizer(key, ";");
		String classname =  st.nextToken();
		
		Class<? extends LocalStructure> matcher_class = null;
		try {
			matcher_class = (Class<? extends LocalStructure>) Class.forName("sci2s.mrfingerprint." + classname);
		} catch (ClassNotFoundException e1) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to create an object of class " + classname + ": class not found");
			e1.printStackTrace();
			return null;
		}
		
		Constructor<?> ctor = null;
		
		try {
			ctor = matcher_class.getConstructor(String.class);
		} catch (NoSuchMethodException e1) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to call the String constructor of class " + classname);
			e1.printStackTrace();
			return null;
		} catch (SecurityException e1) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to call the String constructor of class " + classname);
			e1.printStackTrace();
			return null;
		}
		
		try {
			return (T) ctor.newInstance(new Object[] {value});
		} catch (InstantiationException e) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to create an object of class " + classname);
			e.printStackTrace();
			
			return null;
		} catch (IllegalAccessException e) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to create an object of class " + classname);
			e.printStackTrace();
			
			return null;
		} catch (IllegalArgumentException e) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to create an object of class " + classname);
			e.printStackTrace();
			
			return null;
		} catch (InvocationTargetException e) {
			System.err.println("LocalStructure.getInstanceFromText: error when trying to create an object of class " + classname);
			e.printStackTrace();
			
			return null;
		}
	}
	
	public static ArrayList<Minutia> readXYTFile(String filename) {
		
		ArrayList<Minutia> minutiae = new ArrayList<Minutia>();
		int index = 0;
		int x = 0;
		int y = 0;
		double theta = 0;
		int quality = 0;
		
		BufferedReader br;
		
		try {
			
			// Open the input file
			br = new BufferedReader(new FileReader(filename));

			String line = null;
			StringTokenizer st = null;
			
			// Each line is a minutia
			while((line = br.readLine()) != null) {
				st = new StringTokenizer(line, " ");
				
				// The line must have 4 elements: x, y, theta, quality
				if(st.countTokens() != 4) {
					br.close();
					throw new LSException("readXYTFile: error when reading " + filename +
							": line " + minutiae.size() + " has " + st.countTokens() + 
							" instead of 4");
				}

				x = Integer.parseInt(st.nextToken());
				y = Integer.parseInt(st.nextToken());
				theta = Double.parseDouble(st.nextToken())*2;
				quality = Integer.parseInt(st.nextToken());
				
				minutiae.add(new Minutia(index, x, y, theta, quality));
				
				index++;
			}
			
			br.close();
			
		} catch (FileNotFoundException e) {
			System.err.println("readXYTFile: file " + filename + " not found");
			System.err.println(e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("readXYTFile: error when reading " + filename +
					"(line " + minutiae.size() + ")");
			System.err.println(e.getMessage());
			e.printStackTrace();
		} catch (LSException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		
		return minutiae;
	}
	
	public static ArrayList<Minutia> decodeTextMinutiae(String encoded) {
		
		int index = 0;
		int x = 0;
		int y = 0;
		double theta = 0;
		int quality = 0;
		ArrayList<Minutia> minutiae = new ArrayList<Minutia>();
		
		// The fingerprint ID is the first token before the colon
		StringTokenizer st = new StringTokenizer(encoded, ":");
		StringTokenizer mintk = null;
		
		// Discard the fingerprint ID
		String fpid = st.nextToken();
		
		String encoded_minutiae = st.nextToken();
		
		// Minutiae are separated by semicolons 
		st = new StringTokenizer(encoded_minutiae, ";");
		
		try {
		
			while(st.hasMoreTokens()) {
				mintk = new StringTokenizer(st.nextToken(), " ");
				
				// The line must have 4 elements: x, y, theta, quality
				if(mintk.countTokens() != 4) {
					throw new LSException("decodeTextMinutiae: error when reading " + fpid +
							": minutiae " + minutiae.size() + " has " + mintk.countTokens() + 
							" elements instead of 4");
				}
				
				x = Integer.parseInt(mintk.nextToken());
				y = Integer.parseInt(mintk.nextToken());
				theta = Double.parseDouble(mintk.nextToken())*2;
				quality = Integer.parseInt(mintk.nextToken());
				
				minutiae.add(new Minutia(index, x, y, theta, quality));
				
				index++;
			}

		}  catch (LSException e) {

			System.err.println(e.getMessage());
			e.printStackTrace();
		}
		
		
		return minutiae;
	}

	public static <T extends LocalStructure> T [] extractLocalStructures(Class<T> lsclass, String encoded) {
		String fpid = decodeFpid(encoded);
		ArrayList<Minutia> minutiae = decodeTextMinutiae(encoded);
		
		return extractLocalStructures(lsclass, fpid, minutiae);
	}

	// TODO implement the automatic input from xyt files
	public static <T extends LocalStructure> LocalStructure [][] extractLocalStructuresFromFile(Class<T> lsclass, String input_file) {
		
		String[] lines = Util.readFileByLines(input_file);
		LocalStructure [][] result = new LocalStructure[lines.length][];
		
		for(int i = 0; i < lines.length; i++) {
			result[i] = extractLocalStructures(lsclass, lines[i]);
		}

		return result;
	}
	
	@SuppressWarnings("unchecked")
	public static <T extends LocalStructure> T [] extractLocalStructures(Class<T> lsclass, String fpid, ArrayList<Minutia> minutiae) {

		
		if(lsclass == LocalStructureJiang.class) {
		
			double[][] distance_matrix = computeDistance(minutiae);
			int[][] neighborhood = computeNeighborhood(distance_matrix);

			return (T[]) LocalStructureJiang.extractLocalStructures(fpid, minutiae, distance_matrix, neighborhood);
		}

		else if(lsclass == LocalStructureCylinder.class)
			return (T[]) LocalStructureCylinder.extractLocalStructures(fpid, minutiae);
			
		else
			return null;
	}


	protected static int[][] computeNeighborhood(double[][] distance_matrix) {
		int [][] neighborhood = new int[distance_matrix.length][distance_matrix.length-1];
		TreeMap<Double, Integer> map = new TreeMap<Double, Integer>();
	
		for(int i = 0; i < neighborhood.length; i++) {
			
			map.clear();
			
			for(int j = 0; j < distance_matrix[i].length; j++) {
				if(i != j)
					map.put(distance_matrix[i][j], j);	
			}
			
			int j = 0;
			for(Map.Entry<Double, Integer> entry : map.entrySet()) {
				neighborhood[i][j] = entry.getValue();
				j++;
			}
		}
		
		return neighborhood;
	}


	protected static double[][] computeDistance(ArrayList<Minutia> minutiae) {

		double [][] distance_matrix = new double[minutiae.size()][minutiae.size()];

		distance_matrix[0][0] = 0;
		
		// The distance matrix is symmetric
		for(int i = 1; i < distance_matrix.length; i++) {
			for(int j = 0; j < i; j++) {
				distance_matrix[i][j] = minutiae.get(i).getDistance(minutiae.get(j));
				distance_matrix[j][i] = distance_matrix[i][j]; 
			}
			
			distance_matrix[i][i] = 0;
		}
			
		
		return distance_matrix;
	}
	

	public void setFpid(String fpid) {
		this.fpid = fpid;
	}
	
	public void setLSid(int lsid) {
		this.lsid = lsid;
	}
	
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, fpid);
		out.writeInt(lsid);
	}
	
	public void readFields(DataInput in) throws IOException {
		fpid = Text.readString(in);
		lsid = in.readInt();
	}

	public abstract ArrayWritable newArrayWritable(LocalStructure [] ails);
	public abstract ArrayWritable newArrayWritable();

	public static void saveLSMapFile(LocalStructure[][] inputls, Configuration conf) {

		String name = conf.get(Util.MAPFILENAMEPROPERTY, Util.MAPFILEDEFAULTNAME);
		
    	MapFile.Writer lsmapfile = Util.createMapFileWriter(conf, name, Text.class, inputls[0][0].newArrayWritable().getClass());
    	
    	Arrays.sort(inputls, new Comparator<LocalStructure[]>() {
		   public int compare(LocalStructure [] als1, LocalStructure [] als2) {
		      return als1[0].getFpid().compareTo(als2[0].getFpid());
		   }
		});
    	
    	Text fpid = new Text();
    	ArrayWritable aw = null;

		for(LocalStructure [] ails : inputls) {
			fpid.set(ails[0].getFpid());

		    try {
		    	aw = ails[0].newArrayWritable(ails);
		    	lsmapfile.append(fpid, aw);
			} catch (IOException e) {
				System.err.println("LocalStructure.saveLSMapFile: unable to save fingerprint "
						+ fpid.toString() + " in MapFile " + name + ": " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		IOUtils.closeStream(lsmapfile);
		
	}

	public static <T extends LocalStructure> LocalStructure [][] loadLSMapFile(Configuration conf) {

		String name = conf.get(Util.MAPFILENAMEPROPERTY, Util.MAPFILEDEFAULTNAME);
    	MapFile.Reader lsmapfile = Util.createMapFileReader(conf, name);
    	
    	LocalStructure [][] result = null;

		WritableComparable<?> key = (WritableComparable<?>) ReflectionUtils.newInstance(lsmapfile.getKeyClass(), conf);

		ArrayWritable value = (ArrayWritable) ReflectionUtils.newInstance(lsmapfile.getValueClass(), conf);
		
		try {
			while(lsmapfile.next(key, value)) {
				result = (LocalStructure [][]) ArrayUtils.add(result,
						Arrays.copyOf(value.get(), value.get().length, LocalStructure[].class));
			}
		} catch (Exception e) {
			System.err.println("LocalStructure.loadLSMapFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}
		
		IOUtils.closeStream(lsmapfile);
		
		return result;		
	}
	
	
	abstract public double similarity(LocalStructure ls) throws LSException;

}
