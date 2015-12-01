package sci2s.mrfingerprint;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class LSBuilder extends Configured implements Tool{

  public static void main(String[] args) throws Exception {
	  System.exit(ToolRunner.run(new LSBuilder(), args));
  }

public int run(String[] arg0) throws Exception {
    
    Configuration conf = getConf();

    /*
     * Instantiate a Job object for your job's configuration. 
     */
    Job job = Job.getInstance(conf);

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (arg0.length != 2) {
      System.out.printf("Usage: LSBuilder <minutiae list> <output dir>\n");
      System.exit(-1);
    }
    
    System.out.println("Matcher: " + conf.get("matcher"));
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     */
    job.setJarByClass(LSBuilder.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("LSBuilder");

    FileInputFormat.addInputPath(job, new Path(arg0[0]));
    FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.setMapperClass(LSBuilderMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Class.forName("sci2s.mrfingerprint." + conf.get("matcher")));
//    job.setOutputValueClass(GenericLSWrapper.class);
	
	// There are no reducers
	job.setNumReduceTasks(0);
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    return (job.waitForCompletion(true) ? 0 : 1);
}
}

