package sci2s.mrfingerprint;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ProcessInputFingerprints extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ProcessInputFingerprints(), args));
  	}
	
	public static void printUsage() {
		System.err.println("Reads a file containing the minutiae of a set of fingerprints.\n"
				+ "These fingerprints are typically used for input.\n"
				+ "The local strucutres are computed and stored in a MapFile + InfoFile format, according to the given consolidation type.\n");
		System.err.println("Usage: hadoop jar " + ProcessInputFingerprints.class.getName() + " [Options] <input xytl file>\n");
		System.err.println("Options:");
		System.err.println("\t-D matcher={LocalStructureJiang|LocalStructureCylinder}\tNo default");
		System.err.println("\t-D PartialScore={PartialScoreJiang|PartialScoreLSS|PartialScoreLSSR}\tNo default");
		System.err.println("\t-D MapFileName=<file> \tDefault: " + Util.MAPFILEDEFAULTNAME);
		System.err.println("\t-D InfoFileName=<file>\tDefault: " + Util.INFOFILEDEFAULTNAME);
	}

	public int run(String[] arg0) throws Exception {
	    
	    Configuration conf = getConf();
	    
	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Job job = Job.getInstance(conf);
	
	    /*
	     * Validate that all arguments were passed from the command line.
	     */
	    if (arg0.length != 1) {
	    	printUsage();
	    	System.exit(-1);
	    }
	    
	    
	    /*
	     * Process the input fingerprints
	     */
	    processInputFingerprints(arg0[0], job);

	    return 0;
	}

	/**
	 * Performs the necessary operations to store the fingerprint information in the distributed cache 
	 * @param file Name of the input fingerprint file
	 * @param job Job
	 */
	private void processInputFingerprints(String file, Job job) {

	    @SuppressWarnings("unchecked")
		Class<? extends LocalStructure> MatcherClass = (Class<? extends LocalStructure>) Util.getClassFromProperty(getConf(), "matcher");

		LocalStructure [][] inputls = LocalStructure.extractLocalStructuresFromFile(MatcherClass, file);
		
		if(inputls.length == 0) {
			System.err.println("processInputFingerprints: no input local structures could be read");
			return;
		}
		
		// Sort the fingerprints by their ID, they will be easier to retrieve with the MapFile
		Arrays.sort(inputls, new Comparator<LocalStructure[]>() {
		    public int compare(LocalStructure[] a, LocalStructure[] b) {
		        return a[0].getFpid().compareTo(b[0].getFpid());
		    }
		});

	    Configuration conf = job.getConfiguration();
	    
	    // Save the Local Structures in a Map File, to read them efficiently from the Mapper	    
	    try {
	    	Method method = MatcherClass.getMethod("saveLSMapFile", LocalStructure[][].class, Configuration.class);
			method.invoke(null, inputls, conf);
		} catch (NoSuchMethodException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    // Save the necessary information for the Combiner and the Reducer
	    try {
	    	((PartialScore) Util.getClassFromProperty(conf, "PartialScore").newInstance()).saveInfoFile(inputls, conf);
		} catch (InstantiationException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

