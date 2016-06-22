package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;


public class PartialScoreJiang implements PartialScore {
	
	protected LocalStructureJiang lsjv[];
	protected TopN<LocalMatch> lmatches;
	
	public final static int BEST = 5;
	
	
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
			int res = Float.compare(sl, arg0.sl);
			
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
	
	public PartialScoreJiang() {
		lsjv = null;
		
		// Create the local match heap, with inverted order
		lmatches = new TopN<LocalMatch>(BEST);
	}
	
	public PartialScoreJiang(PartialScoreJiang o) {
		lsjv = Util.arraycopy(o.lsjv);
		lmatches = new TopN<LocalMatch>(o.lmatches);
	}
	

	public PartialScoreJiang(LocalStructure ls, LocalStructure[] als) {
		computePartialScore(ls, als);
	}
	
	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreJiang(Iterable<GenericPSWrapper> values) {
		partialAggregateG(values);
	}
	
	@Override
	public PartialScoreJiang clone() {
		PartialScoreJiang ps = new PartialScoreJiang(this);
		return ps;
	}
	
	public LocalStructureJiang[] getLocalStructures() {
		return Util.arraycopy(lsjv);
	}
	
	@Override
	public String toString() {
		String res = super.toString();
		
		res += lsjv.length;
		
		for(LocalStructureJiang lsj : lsjv)
			res += ";" + lsj.toString();
		

		for(LocalMatch tmp : lmatches)
			res += ";" + "(" + tmp.b1 + "," + tmp.b2 + "," + tmp.sl + ")";
		
		return res;
	}

	public void readFields(DataInput in) throws IOException {

		ArrayWritable auxaw = new ArrayWritable(LocalStructureJiang.class);

		// Read the template local structures
		auxaw.readFields(in);
		Writable [] writables = auxaw.get();
		lsjv = new LocalStructureJiang [writables.length];
		
		for(int i = 0; i < writables.length; i++)
			lsjv[i] = (LocalStructureJiang) writables[i];
		
		// Read the local matches
		auxaw = new ArrayWritable(LocalMatch.class);
		auxaw.readFields(in);
		
		for(Writable w : auxaw.get())
			lmatches.add((LocalMatch) w);
	}

	public void write(DataOutput out) throws IOException {

		// Write the template local structures
		ArrayWritable auxaw = new ArrayWritable(LocalStructureJiang.class, lsjv);
		auxaw.write(out);
		
		// Write the local matches
		auxaw = new ArrayWritable(LocalMatch.class, lmatches.toArray(new Writable[0]));
		auxaw.write(out);
	}


	public float aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		PartialScoreJiang bestps = new PartialScoreJiang(values);

		return bestps.computeScore(key.getFpidInput().toString(), infomap);
	}
	

	public static Map<String, LocalStructureJiang[]> loadInfoFile(Configuration conf) {
    	
    	Map<String, LocalStructureJiang[]> infomap = new HashMap<String, LocalStructureJiang[]>();
    	
    	LocalStructureJiang [][] ilsarray = LocalStructureJiang.loadLSMapFile(conf);
    	
    	for(LocalStructureJiang[] ails : ilsarray) {
    		infomap.put(ails[0].getFpid(), ails);
    	}
		
		return infomap;
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		// Does nothing
		return null;
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureJiang.class);
	}

	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {
		// The necessary information is already in the Local Structure MapFile
		return;
	}

	public void computePartialScore(LocalStructure ls,
			LocalStructure[] als) {

		lsjv = new LocalStructureJiang[1];
		lsjv[0] = (LocalStructureJiang) ls;
		lmatches = new TopN<LocalMatch>(BEST);

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
	}

	public void partialAggregateG(PartialScoreKey key,
			Iterable<GenericPSWrapper> values, Map<?, ?> infomap) {

		partialAggregateG(values);
	}

	public void partialAggregateG(Iterable<GenericPSWrapper> values) {


		// Initialize member variables
		lsjv = null;
		lmatches = new TopN<LocalMatch>(BEST);
		PartialScoreJiang psj;
				
		for(GenericPSWrapper ps : values) {
			psj = (PartialScoreJiang) ps.get();
			lmatches.addAll(psj.lmatches);
			lsjv = (LocalStructureJiang[]) ArrayUtils.addAll(lsjv, psj.lsjv);
		}
	}

	public boolean isEmpty() {		
		// All the local structures are needed for the consolidation, so they cannot be discarded
		return false;
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {
	
		PartialScoreJiang psj = (PartialScoreJiang) ps;
		
		// Initialize member variables
		lmatches.addAll(psj.lmatches);
		lsjv = (LocalStructureJiang[]) ArrayUtils.addAll(lsjv, psj.lsjv);
		
		return this;
	}

	public float computeScore(String input_fpid, Map<?, ?> infomap) {
		return computeScore((LocalStructureJiang[]) infomap.get(input_fpid));
	}

	public float computeScore(LocalStructureJiang [] inputls) {

		// If the most similar pair has similarity 0, no need to say more
		if(lmatches.isEmpty() || lmatches.first().sl == 0)
			return 0.0f;
		
		LocalStructureJiang [] templatels = lsjv;
	
		// Sort the local structures, so that the LSID corresponds to the position in the array
		Arrays.sort(templatels);
		Arrays.sort(inputls);
		
		// Arrays for the transformed minutiae
		float[][] template_Fg = new float[templatels.length][];
		float[][] input_Fg = new float[inputls.length][];

		float maxmlsum = 0.0f;
		PriorityQueue<LocalMatch> ml = new PriorityQueue<LocalMatch>(20, Collections.reverseOrder());

		
		Set<Integer> used_template = new HashSet<Integer>(templatels.length);
		Set<Integer> used_input = new HashSet<Integer>(inputls.length);
		
		LocalMatch lm;

		// For each of the BEST best local matchings
		while((lm = lmatches.poll()) != null) {
			
			// Get the best minutia of each fingerprint
			Minutia best_minutia_template = templatels[lm.b1].getMinutia();
			Minutia best_minutia_input = inputls[lm.b2].getMinutia();
			
			// Transform all the LS of both fingerprints using the best minutiae pair.
			for(int i = 0; i < templatels.length; i++) {
				template_Fg[i] = templatels[i].transformMinutia(best_minutia_template);
			}
			for(int i = 0; i < inputls.length; i++) {
				input_Fg[i] = inputls[i].transformMinutia(best_minutia_input);
			}
			
			// Compute "ml" matrix, for all the pairs of transformed minutiae.	
			ml.clear();
			
			for(int i = 0; i < templatels.length; i++)
				for(int j = 0; j < inputls.length; j++) {
					
					boolean out = false;
					
					for(int k = 0; k < 3; k++)
						out = out || (Math.abs(template_Fg[i][k] - input_Fg[j][k]) >= LocalStructureJiang.BG[k]);
		
					if(!out)
						try {
							ml.add(new LocalMatch(i, j, 0.5f + 0.5f*templatels[i].similarity(inputls[j])));
						} catch (LSException e) {									
							System.err.println("MatchingReducer.reduce: error when computing the similarity for minutiae (" +
									templatels[i].getFpid() + "," + templatels[i].getLSid() + ") and (" +
						inputls[j].getFpid() + "," + inputls[j].getLSid() + ")");
							e.printStackTrace();
						}
					
				}
	
			// Compute the sum of "ml", avoiding to use the same minutia twice.
			float mlsum = 0.0f;
			LocalMatch bestlm = null;
			
			used_template.clear();
			used_input.clear();

			while((bestlm = ml.poll()) != null) {
				
				if(!used_template.contains(bestlm.b1) && !used_input.contains(bestlm.b2)) {
					mlsum += bestlm.sl;
					
					used_template.add(bestlm.b1);
					used_input.add(bestlm.b2);
				}
			}

			if(mlsum > maxmlsum)
				maxmlsum = mlsum;
		}
		
		return maxmlsum/Math.max(templatels.length, inputls.length);
	}
	
//	public float computeScore(Collection<LocalStructureJiang> inputls) {
//		if(inputls instanceof ArrayList)
//			return computeScore(inputls);
//		else
//			return computeScore(new ArrayList<LocalStructureJiang> (inputls));
//	}
}
