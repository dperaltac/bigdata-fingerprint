package sci2s.mrfingerprint;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LSBuilderMapper extends Mapper<LongWritable, Text, Text, LocalStructure> {

  @SuppressWarnings("unchecked")
@Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	 
	  Configuration conf = context.getConfiguration();
	  Class<? extends LocalStructure> MatcherClass ;
	  
	  try {
		  MatcherClass = (Class<? extends LocalStructure>) Class.forName("sci2s.mrfingerprint." + conf.get("matcher"));
	  } catch (ClassNotFoundException e) {
		  System.err.println("LSBuilderMapper::map: class " + " not found. Using LocalStructureJiang as default.");
		  e.printStackTrace();
		  MatcherClass = LocalStructureJiang.class;
	  }
	  
	  boolean discarding = (context.getConfiguration().getBoolean("discarding", false));

	  LocalStructure [] lslist = LocalStructure.extractLocalStructures(MatcherClass, value.toString(), discarding);		  
	  
	  for(LocalStructure ls : lslist) {
		  context.write(new Text(MatcherClass.getName() + ";" + ls.getFpid()), ls);
//		  context.write(new Text(MatcherClass.getName() + ";" + ls.getFpid()), new GenericLSWrapper(ls));
	  }
  }
  
}
