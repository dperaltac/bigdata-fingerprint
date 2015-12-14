package sci2s.resultsanalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class ResultsAnalyzerDetailed extends ResultsAnalyzer {

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ResultsAnalyzerDetailed(), args));
	}

	public int run(String[] arg0) throws Exception {

		/*
		 * Validate that all arguments were passed from the command line.
		 */
		if (arg0.length != 3) {
			printUsage();
			System.exit(-1);
		}

		Configuration conf = getConf();
		conf.set("database", arg0[2]);
		/*
		 * Instantiate a Job object for your job's configuration. 
		 */
		Job job = Job.getInstance(conf);

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running 
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(ResultsAnalyzerDetailed.class);

		/*
		 * Specify an easily-decipherable name for the job.
		 * This job name will appear in reports and logs.
		 */
		job.setJobName("ResultsAnalyzerDetailed");

		Path outpath = new Path(arg0[1]);

		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, outpath);

		outpath.getFileSystem(conf).delete(outpath, true);

		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(ResultsAnalyzerMapper.class);
		job.setReducerClass(ResultsAnalyzerReducerDetailed.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ScorePair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		return (job.waitForCompletion(true) ? 0 : 1);
	}

}

