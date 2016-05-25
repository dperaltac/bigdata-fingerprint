package sci2s.mrfingerprint;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.special.Erf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Util {

	public static final double RADTOREG = 57.324840764331210191082802547771; // Radians to degrees
	public static final double REGTORAD = 0.017453293; // Radians to degrees

	public final static String MAPFILEDEFAULTNAME = "InputLocalStructures.MapFile";
	public final static String MAPFILENAMEPROPERTY = "MapFileName";
	public final static String INFOFILEDEFAULTNAME = "InputLocalStructures.InfoFile";
	public final static String INFOFILENAMEPROPERTY = "InfoFileName";
	
	private Util() {};
	
	public static double dFi(double a, double b) {
		double diff = a-b;
		
		if(diff > Math.PI)
			return 2*Math.PI - diff;
		else if(diff <= -Math.PI)
			return 2*Math.PI + diff;
		else
			return diff;
	}
	
	public static double dFiMCC(double a, double b) {
		double diff = a-b;
		
	    if (diff >= -Math.PI && diff < Math.PI)
	        return diff;
	    else if (diff < -Math.PI)
	        return 2*Math.PI + diff;
	    else
	        return -2*Math.PI + diff;
	}
	
	public static double square(double a) {
		return a*a;
	}
	
	public static <T> T[] arraycopy(T[] a) {

		return Arrays.copyOf(a, a.length);
		
	}
	
	public static <T> T[][] arraycopy2d(T[][] a) {
		@SuppressWarnings("unchecked")
		T[][] b = (T[][]) java.lang.reflect.Array.newInstance(a[0][0].getClass(), new int[]{a.length, a[0].length});
		
		for(int i = 0; i < a.length; i++)
			b[i] = Arrays.copyOf(a[i], a[i].length);
		
		return b;
		
	}
	
	// TODO this is a potential bottleneck with real big data.
	public static String[] readFileByLines(String filename) {

		ArrayList<String> result = new ArrayList<String>();
		BufferedReader br = null;
		String line;
		
		try {
			br = new BufferedReader(new FileReader(filename));

			while ((line = br.readLine()) != null) {
				result.add(line);
			}

			br.close();
		} catch (FileNotFoundException e) {
			System.err.println("Util.readFileByLines: file " + filename + " was not found");
			e.printStackTrace();
			return new String[0];
		} catch (IOException e) {
			System.err.println("Util.readFileByLines: error when reading file " + filename);
			e.printStackTrace();
		}
		
		return result.toArray(new String[result.size()]);
	}
	
	public static double doLeft(double z)
	{
		return (1.0+Erf.erf(z*0.707106781))/2;
	}
	
	public static double DistanceFromLine(double cx, double cy, double ax, double ay ,
	                                          double bx, double by)
	{
		double diffbax = bx-ax;
		double diffbay = by-ay;
		double diffcax = cx-ax;
		double diffcay = cy-ay;
		double diffcbx = cx-bx;
		double diffcby = cy-by;
		double r_numerator = diffcax*diffbax + diffcay*diffbay;
		double r_denomenator = square(diffbax) + square(diffbay);

	//
	// (xx,yy) is the point on the lineSegment closest to (cx,cy)
	//

		if ( (r_numerator >= 0) && (r_numerator <= r_denomenator) )
		{
			return Math.abs(diffcax*diffbay - diffcay*diffbax) / Math.sqrt(r_denomenator);
		}
		else
		{
			double dist1 = square(diffcax) + square(diffcay);
			double dist2 = square(diffcbx) + square(diffcby);

			return Math.sqrt(Math.min(dist1, dist2));
		}
	}


	
	public static Class<?> getClassFromProperty(JobContext context, String name) {

		  return getClassFromProperty(context.getConfiguration(), name);
	}
	
	public static Class<?> getClassFromProperty(Configuration conf, String name) {

		  Class<?> myclass;
		  String value = conf.get(name);
		  
		  if(value == null) {
			  System.err.println("Util::getClassFromProperty: property " + name + " not found.");
			  return null;
		  }
		  
		  try {
			  myclass = Class.forName("sci2s.mrfingerprint." + value);
		  } catch (ClassNotFoundException e) {
			  System.err.println("Util::getClassFromProperty: class " + value + " not found.");
			  e.printStackTrace();
			  myclass = null;
		  }
		  
		  return myclass;
	}
	
	public static <T extends Comparable<T>> Integer [] sortIndexes(final T [] array) {
		
		Integer [] idx = new Integer[array.length];
		
		for(int i = 0; i < array.length; i++)
			idx[i] = i;

		Arrays.sort(idx, new Comparator<Integer>() {
		    public int compare(final Integer o1, final Integer o2) {
		        return array[o1].compareTo(array[o2]);
		    }
		});
		
		return idx;
	}
	
	/**
	 * Similar to the "order" function in R. Returns the indices of the sorted array.
	 * @param array Array whose order is calculated
	 * @return Array of indices for the ordered vector. The first index is the minimum value, and so forth.
	 */
	public static Integer [] sortIndexes(final double [] array) {
		
		Integer [] idx = new Integer[array.length];
		
		for(int i = 0; i < array.length; i++)
			idx[i] = i;

		Arrays.sort(idx, new Comparator<Integer>() {
		    public int compare(final Integer o1, final Integer o2) {
		        return Double.compare(array[o1], array[o2]);
		    }
		});
		
		return idx;
	}
	
    public static double psi(double v, double par1, double par2)
    {
    	return 1.0 / (1.0 + Math.exp((par2*(par1-v))));
    }
    

	public static LocalStructure [][] readDistributedCacheFingerprints(URI[] input_files, Class<? extends LocalStructure> MatcherClass) throws IOException {
	    
	    LocalStructure [][] inputls = null;

	    // Compute the localstructures of the input fingerprint
	    // and store so that all maps and reduces can access.
	    for(int i = 0; i < input_files.length; i++){
//	    	inputls.addAll(LocalStructure.extractLocalStructuresFromFile(MatcherClass, FilenameUtils.getName(input_file.getPath())));
	    	LocalStructure [][] ials = LocalStructure.extractLocalStructuresFromFile(MatcherClass, input_files[i].getPath());
	    	inputls = (LocalStructure[][]) ArrayUtils.addAll(inputls, ials);
	    }
	    
	    return inputls;
	}
	


	@SuppressWarnings("rawtypes")
	public static LocalStructure [] readDistributedCacheFingerprint(Context context, String fpid) throws IOException {

	    URI[] input_files = context.getCacheFiles();
	    
		@SuppressWarnings("unchecked")
		Class<? extends LocalStructure> MatcherClass = (Class<? extends LocalStructure>) Util.getClassFromProperty(context, "matcher");

	    // Compute the localstructures of the input fingerprint
	    // and store so that all maps and reduces can access.
	    for(URI input_file : input_files) {
//			String[] lines = Util.readFileByLines(FilenameUtils.getName(input_file.getPath()));
			String[] lines = Util.readFileByLines(input_file.getPath());

			for(String line : lines) {
				if(LocalStructure.decodeFpid(line).equals(fpid))
					return LocalStructure.extractLocalStructures(MatcherClass, line);
			}
	    }
	    
	    System.err.println("readDistributedCacheFingerprint: input fingerprint " + fpid + " not found");
	    return null;
	}
	

	

	@SuppressWarnings("rawtypes")
	public static MapFile.Writer createMapFileWriter(Configuration conf, String name, Class<? extends WritableComparable> keyclass, Class<? extends Writable> valueclass) {

    	MapFile.Writer file = null;
    	
    	try {
    		file = new MapFile.Writer(conf, new Path(name),
    				Writer.keyClass(keyclass), Writer.valueClass(valueclass));
    		
		} catch (IllegalArgumentException e1) {
			System.err.println("Util.createMapFileWriter " + name +
					": " + e1.getMessage());
			e1.printStackTrace();
		} catch (IOException e1) {
			System.err.println("Util.createMapFileWriter: unable to create MapFile " + name +
					": " + e1.getMessage());
			e1.printStackTrace();
		}
		
		return file;
	}

	public static MapFile.Reader createMapFileReader(Configuration conf, String name) {

    	MapFile.Reader file = null;
    	
    	try {
    		file = new MapFile.Reader(new Path(name), conf);
    		
		} catch (IOException e1) {
			System.err.println("Util.createMapFileWriter: unable to read MapFile " + name +
					": " + e1.getMessage());
			e1.printStackTrace();
		}
		
		return file;
	}
	
	
	public static int minPosition(double [] v) {
		int pos = 0;
		double min = Double.MAX_VALUE;
		
		for(int i = 0; i < v.length; ++i) {
			if(v[i] < min) {
				pos = i;
				min = v[i];
			}
		}
		
		return pos;
	}
	
	
	public static int maxPosition(double [] v) {
		int pos = 0;
		double max = Double.MIN_VALUE;
		
		for(int i = 0; i < v.length; ++i) {
			if(v[i] > max) {
				pos = i;
				max = v[i];
			}
		}
		
		return pos;
	}
}
