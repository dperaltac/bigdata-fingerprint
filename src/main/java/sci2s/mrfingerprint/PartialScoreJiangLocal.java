package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;


public class PartialScoreJiangLocal implements PartialScore {
	
	protected SortedSet<LocalMatch> lmatches;
	int templatesize;
	
	static protected class LocalMatch implements Comparable<LocalMatch>, Writable {

		public int b1;
		public int b2;
		public float sl;

		public LocalMatch() {
			b1 = 0;
			b2 = 0;
			sl = 0.0f;
		}
		
		public LocalMatch(LocalMatch o) {
			b1 = o.b1;
			b2 = o.b2;
			sl = o.sl;
		}
		
		public LocalMatch(int b1, int b2, float sl) {
			this.b1 = b1;
			this.b2 = b2;
			this.sl = sl;
		}

		public int compareTo(LocalMatch arg0) {
			int res = Double.compare(sl, arg0.sl);
			
			if(res == 0) {
				res = Integer.compare(b1,arg0.b1);
				
				if(res == 0)
					return Integer.compare(b2,arg0.b2);
			}
				
			return res;
		}

		public void readFields(DataInput arg0) throws IOException {
			b1 = arg0.readInt();
			b2 = arg0.readInt();
			sl = arg0.readFloat();
		}

		public void write(DataOutput arg0) throws IOException {
			arg0.writeInt(b1);
			arg0.writeInt(b2);
			arg0.writeFloat(sl);
		}
	}
	
	public PartialScoreJiangLocal() {
		
		// Create the local match heap, with inverted order
		lmatches = new TreeSet<LocalMatch>(Collections.reverseOrder());
		templatesize = 0;
	}
	
	public PartialScoreJiangLocal(PartialScoreJiangLocal o) {
		lmatches = new TreeSet<LocalMatch>(o.lmatches);
		templatesize = o.templatesize;
	}
	

	public PartialScoreJiangLocal(LocalStructure ls, LocalStructure[] als) {

		lmatches = new TreeSet<LocalMatch>(Collections.reverseOrder());

		// Get the input local structure with maximum similarity w.r.t. the single template local structure
		for(LocalStructure ils : als) {

			try {
				float sl = ls.similarity(ils);
				
				if(sl > 0.0)
					lmatches.add(new LocalMatch(ls.getLSid(), ils.getLSid(), sl));

			} catch (LSException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
		
		templatesize = 1;
		
		reduceLmatches();
	}
	
	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreJiangLocal(Iterable<GenericPSWrapper> values) {

		templatesize = 0;
		
		// Initialize member variables
		lmatches = new TreeSet<LocalMatch>(Collections.reverseOrder());
		PartialScoreJiangLocal psj;
				
		for(GenericPSWrapper ps : values) {
			psj = (PartialScoreJiangLocal) ps.get();
			lmatches.addAll(psj.lmatches);
			templatesize += psj.templatesize; 
		}
		
		reduceLmatches();
	}
	
	@Override
	public String toString() {
		String res = super.toString();
		

		for(LocalMatch tmp : lmatches)
			res += "(" + tmp.b1 + "," + tmp.b2 + "," + tmp.sl + "); ";
		
		return res;
	}

	public void readFields(DataInput in) throws IOException {
		
		templatesize = in.readInt();
		
		// Read the local matches
		ArrayWritable auxaw = new ArrayWritable(LocalMatch.class);
		auxaw.readFields(in);
		
		for(Writable w : auxaw.get())
			lmatches.add((LocalMatch) w);
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeInt(templatesize);
		
		// Write the local matches
		ArrayWritable auxaw = new ArrayWritable(LocalMatch.class, lmatches.toArray(new Writable[0]));
		auxaw.write(out);
	}


	public float aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		PartialScoreJiangLocal bestps = new PartialScoreJiangLocal(values);

		return bestps.computeScore();
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		// Does nothing
		return null;
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return null;
	}

	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureJiang.class);
	}

	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {
		// The necessary information is already in the Local Structure MapFile
		return;
	}

	public PartialScore computePartialScore(LocalStructure ls,
			LocalStructure[] als) {
		return new PartialScoreJiangLocal(ls, als);
	}

	public PartialScore partialAggregateG(PartialScoreKey key,
			Iterable<GenericPSWrapper> values, Map<?, ?> infomap) {

		return new PartialScoreJiangLocal(values);
	}

	public boolean isEmpty() {		
		// All the local structures are needed for the consolidation, so they cannot be discarded
		return false;
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {
	
		PartialScoreJiangLocal psj = (PartialScoreJiangLocal) ps;
		
		// Initialize member variables
		lmatches.addAll(psj.lmatches);
		
		templatesize += psj.templatesize;
		
		reduceLmatches();
		
		return this;
	}
	
	protected void reduceLmatches() {

		// If the most similar pair has similarity 0, no need to say more
		if(lmatches.isEmpty())
			return;
		
		if(lmatches.first().sl == 0) {
			lmatches.clear();
			return;
		}
		
		Iterator<LocalMatch> it = lmatches.iterator();
		Set<Integer> used_template = new HashSet<Integer>();
		Set<Integer> used_input = new HashSet<Integer>();
		
		while(it.hasNext()) {
			LocalMatch lm = it.next();
			
			// The set lmatches is sorted, so no need to check further
			if(used_template.contains(lm.b1) || used_input.contains(lm.b2))
				it.remove();
			else {
				used_template.add(lm.b1);
				used_input.add(lm.b2);
			}
		}
	}

	public float computeScore(String input_fpid, Map<?, ?> infomap) {
		
		return computeScore();
	}

	public float computeScore() {
		
		float score = 0.0f;
		
		reduceLmatches();
		
		for(LocalMatch lm : lmatches)
			score += lm.sl;
		
		return(score / templatesize);
	}
}
