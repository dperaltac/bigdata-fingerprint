package sci2s.mrfingerprint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRMatcher extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MRMatcher(), args));
  	}
	
	public static void printUsage() {
		System.err.println("Identifies the input fingerprints stored in the MapFile and InfoFile within the given template file.\n"
				+ "The input fingerprints should be first computed with ProcessInputFingerprints.\n");
		System.err.println("Usage: hadoop jar " + MRMatcher.class.getName() + " [Options] <local structure template file> <output dir>\n");
		System.err.println("Options:");
		System.err.println("\t-D PartialScore={PartialScoreJiang|PartialScoreLSS|PartialScoreLSSR}\tNo default");
		System.err.println("\t-D MapFileName=<file> \tDefault: " + Util.MAPFILEDEFAULTNAME);
		System.err.println("\t-D InfoFileName=<file>\tDefault: " + Util.INFOFILEDEFAULTNAME);
	}

	public int run(String[] arg0) throws Exception {
	    
	    Configuration conf = getConf();
	    
//	    conf.setBoolean(MRJobConfig.TASK_PROFILE, true);
//	    conf.set(MRJobConfig.TASK_PROFILE_PARAMS, "-agentlib:hprof=cpu=samples," +
//	    "heap=sites,depth=10,force=n,thread=y,verbose=n,file=%s");
//	    conf.set(MRJobConfig.NUM_MAP_PROFILES, "0-1");
//	    conf.set(MRJobConfig.NUM_REDUCE_PROFILES, "0-1");
//	    
//
//	    conf.set("yarn.nodemanager.sleep-delay-before-sigkill.ms", "30000");
//	    conf.set("yarn.nodemanager.process-kill-wait.ms", "30000");
//	    conf.set("mapreduce.tasktracker.tasks.sleeptimebeforesigkill", "30000");

	    conf.setBoolean("mapreduce.map.output.compress", true);
	    /*
	     * Instantiate a Job object for your job's configuration. 
	     */
	    Job job = Job.getInstance(conf);
	
	    /*
	     * Validate that all arguments were passed from the command line.
	     */
	    if (arg0.length != 2) {
	    	printUsage();
	    	System.exit(-1);
	    }
	    
	    /*
	     * Specify the jar file that contains your driver, mapper, and reducer.
	     * Hadoop will transfer this jar file to nodes in your cluster running 
	     * mapper and reducer tasks.
	     */
	    job.setJarByClass(MRMatcher.class);
	    
	    
	    /*
	     * Process the input fingerprints
	     */
//	    processInputFingerprints(arg0[1], job);
	    
	    /*
	     * Specify an easily-decipherable name for the job.
	     * This job name will appear in reports and logs.
	     */
	    job.setJobName("GenericMatcher");

	    FileInputFormat.addInputPath(job, new Path(arg0[0]));
	    FileOutputFormat.setOutputPath(job, new Path(arg0[1] + System.currentTimeMillis()));
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    
	    job.setMapperClass(MatchingMapper.class);
	    job.setCombinerClass(MatchingCombiner.class);
	    job.setReducerClass(MatchingReducer.class);
	
		job.setMapOutputKeyClass(PartialScoreKey.class);
		job.setMapOutputValueClass(GenericPSWrapper.class);

	    job.setOutputKeyClass(DoubleWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    /*
	     * Start the MapReduce job and wait for it to finish.
	     * If it finishes successfully, return 0. If not, return 1.
	     */
	    return (job.waitForCompletion(true) ? 0 : 1);
	}
}

