package sci2s.mrfingerprint;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;


@SuppressWarnings("rawtypes")
public class CounterReducer extends Reducer<WritableComparable, Writable, WritableComparable, IntWritable> {


	@Override
	public void reduce(WritableComparable key, Iterable<Writable> values, Context context) 
			throws IOException, InterruptedException {
		
		int count = 0;
		Iterator<Writable> it = values.iterator();
		while(it.hasNext()) {
			count++;
			it.next();
		}
		
		context.write(key, new IntWritable(count));
	}
}
