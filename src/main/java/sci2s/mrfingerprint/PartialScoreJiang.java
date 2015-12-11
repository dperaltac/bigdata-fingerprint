package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.zookeeper.common.IOUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.MinMaxPriorityQueue.Builder;


public class PartialScoreJiang implements PartialScore {
	
	protected LocalStructureJiang lsjv[];
	protected MinMaxPriorityQueue<LocalMatch> lmatches;
	
	public final static int BEST = 5;
	
	
	static protected class LocalMatch implements Comparable<LocalMatch>, Writable {

		public int b1;
		public int b2;
		public double sl;

		public LocalMatch() {
			b1 = 0;
			b2 = 0;
			sl = 0.0;
		}
		
		public LocalMatch(LocalMatch o) {
			b1 = o.b1;
			b2 = o.b2;
			sl = o.sl;
		}
		
		public LocalMatch(int b1, int b2, double sl) {
			this.b1 = b1;
			this.b2 = b2;
			this.sl = sl;
		}

		public int compareTo(LocalMatch arg0) {
			return Double.compare(sl, arg0.sl);
		}

		public void readFields(DataInput arg0) throws IOException {
			b1 = arg0.readInt();
			b2 = arg0.readInt();
			sl = arg0.readDouble();
		}

		public void write(DataOutput arg0) throws IOException {
			arg0.writeInt(b1);
			arg0.writeInt(b2);
			arg0.writeDouble(sl);
		}
		
		protected final static Comparator<LocalMatch> inverted_comparator = new Comparator<LocalMatch>(){
					public int compare(LocalMatch arg0, LocalMatch arg1) {
						int res = arg1.compareTo(arg0);
						
						if(res == 0) {
							res = Integer.compare(arg0.b1,arg1.b1);
							
							if(res == 0)
								return Integer.compare(arg0.b2,arg1.b2);
						}
							
						return res;
					}
				};
				
		public static Comparator<LocalMatch> invertedComparator() {
			return inverted_comparator;
		}
	}
	
	protected static final Builder<LocalMatch> PriorityQueueBuilder = MinMaxPriorityQueue.orderedBy(LocalMatch.invertedComparator()).maximumSize(BEST);
	
	public PartialScoreJiang() {
		lsjv = null;
		
		// Create the local match heap, with inverted order
		lmatches = PriorityQueueBuilder.create();
	}
	
	public PartialScoreJiang(PartialScoreJiang o) {
		lsjv = Util.arraycopy(o.lsjv);
		lmatches = PriorityQueueBuilder.create(lmatches);
	}
	

	public PartialScoreJiang(LocalStructure ls, LocalStructure[] als) {

		lsjv = new LocalStructureJiang[1];
		lsjv[0] = (LocalStructureJiang) ls;
		lmatches = PriorityQueueBuilder.create();

		// Get the input local structure with maximum similarity w.r.t. the single template local structure
		for(LocalStructure ils : als) {

			try {
				double sl = ls.similarity(ils);
				
				if(sl > 0.0)
					lmatches.add(new LocalMatch(ls.getLSid(), ils.getLSid(), sl));

			} catch (LSException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	// Parameter constructor. Performs the partialAggregate operation.
//	public PartialScoreJiang(Iterable<PartialScore> values) {
//
//		// Initialize member variables
//		lsjv = null;
//		lmatches = PriorityQueueBuilder.create();
//				
//		for(PartialScore ps : values) {
//			lmatches.addAll(((PartialScoreJiang) ps).lmatches);
//			lsjv = (LocalStructureJiang[]) ArrayUtils.addAll(lsjv, ((PartialScoreJiang) ps).lsjv);
//		}
//	}
	
	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreJiang(Iterable<GenericPSWrapper> values) {

		// Initialize member variables
		lsjv = null;
		lmatches = PriorityQueueBuilder.create();
		PartialScoreJiang psj;
				
		for(GenericPSWrapper ps : values) {
			psj = (PartialScoreJiang) ps.get();
			lmatches.addAll(psj.lmatches);
			lsjv = (LocalStructureJiang[]) ArrayUtils.addAll(lsjv, psj.lsjv);
		}
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


//	public double aggregate(PartialScoreKey key, Iterable<PartialScore> values, Map<?,?> infomap) {
//
//		PartialScoreJiang bestps = new PartialScoreJiang(values);
//
//		// If the most similar pair has similarity 0, no need to say more
//		if(bestps.lmatches.size() == 0.0 || bestps.lmatches.peek().sl == 0)
//			return 0.0;
//		
//		ArrayList<LocalStructureJiang> templatels = Lists.newArrayList(bestps.getLocalStructures());
//		
//		@SuppressWarnings("unchecked")
//		ArrayList<LocalStructureJiang> inputls = (ArrayList<LocalStructureJiang>) infomap.get(key.getFpidInput().toString());
//		
//	
//		// Sort the local structures, so that the LSID corresponds to the position in the array
//		Collections.sort(templatels);
//		Collections.sort(inputls);
//		
//		// Arrays for the transformed minutiae
//		double[][] template_Fg = new double[templatels.size()][];
//		double[][] input_Fg = new double[inputls.size()][];
//
//		double maxmlsum = 0.0;
////		double ml2[][] = new double[templatels.size()][inputls.size()];
//		PriorityQueue<LocalMatch> ml = new PriorityQueue<LocalMatch>(20, LocalMatch.invertedComparator());
//
//		
//		Set<Integer> used_template = new HashSet<Integer>(templatels.size());
//		Set<Integer> used_input = new HashSet<Integer>(inputls.size());
//		
//		LocalMatch lm;
//
//		// For each of the BEST best local matchings
//		while((lm = bestps.lmatches.poll()) != null) {
//			
//			// Get the best minutia of each fingerprint
//			Minutia best_minutia_template = templatels.get(lm.b1).getMinutia();
//			Minutia best_minutia_input = inputls.get(lm.b2).getMinutia();
//			
//			// Transform all the LS of both fingerprints using the best minutiae pair.
//			for(int i = 0; i < templatels.size(); i++) {
//				template_Fg[i] = templatels.get(i).transformMinutia(best_minutia_template);
//			}
//			for(int i = 0; i < inputls.size(); i++) {
//				input_Fg[i] = inputls.get(i).transformMinutia(best_minutia_input);
//			}
//			
//			// Compute "ml" matrix, for all the pairs of transformed minutiae.	
//			ml.clear();
//			
//			for(int i = 0; i < templatels.size(); i++)
//				for(int j = 0; j < inputls.size(); j++) {
//					
//					boolean out = false;
//					
//					for(int k = 0; k < 3; k++)
//						out = out || !(Math.abs(template_Fg[i][k] - input_Fg[j][k]) < LocalStructureJiang.BG[k]);
//		
//					if(!out)
//						try {
//							ml.add(new LocalMatch(i, j, 0.5 + 0.5*templatels.get(i).similarity(inputls.get(j))));
//						} catch (LSException e) {									
//							System.err.println("MatchingReducer.reduce: error when computing the similarity for minutiae (" +
//									templatels.get(i).getFpid() + "," + templatels.get(i).getLSid() + ") and (" +
//						inputls.get(j).getFpid() + "," + inputls.get(j).getLSid() + ")");
//							e.printStackTrace();
//						}
//					
//				}
//	
//			// Compute the sum of "ml", avoiding to use the same minutia twice.
//			double mlsum = 0.0;
//			LocalMatch bestlm = null;
//			
//			used_template.clear();
//			used_input.clear();
//
//			while((bestlm = ml.poll()) != null) {
//				
//				if(!used_template.contains(bestlm.b1) && !used_input.contains(bestlm.b2)) {
//					mlsum += bestlm.sl;
//					
//					used_template.add(bestlm.b1);
//					used_input.add(bestlm.b2);
//				}
//			}
//
//			if(mlsum > maxmlsum)
//				maxmlsum = mlsum;
//		}
//		
//		return maxmlsum/Math.max(templatels.size(), inputls.size());
//	}
	



	public double aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		PartialScoreJiang bestps = new PartialScoreJiang(values);

		// If the most similar pair has similarity 0, no need to say more
		if(bestps.lmatches.size() == 0.0 || bestps.lmatches.peek().sl == 0)
			return 0.0;
		
		ArrayList<LocalStructureJiang> templatels = Lists.newArrayList(bestps.getLocalStructures());
		
		@SuppressWarnings("unchecked")
		ArrayList<LocalStructureJiang> inputls = (ArrayList<LocalStructureJiang>) infomap.get(key.getFpidInput().toString());
		
	
		// Sort the local structures, so that the LSID corresponds to the position in the array
		Collections.sort(templatels);
		Collections.sort(inputls);
		
		// Arrays for the transformed minutiae
		double[][] template_Fg = new double[templatels.size()][];
		double[][] input_Fg = new double[inputls.size()][];

		double maxmlsum = 0.0;
//		double ml2[][] = new double[templatels.size()][inputls.size()];
		PriorityQueue<LocalMatch> ml = new PriorityQueue<LocalMatch>(20, LocalMatch.invertedComparator());

		
		Set<Integer> used_template = new HashSet<Integer>(templatels.size());
		Set<Integer> used_input = new HashSet<Integer>(inputls.size());
		
		LocalMatch lm;

		// For each of the BEST best local matchings
		while((lm = bestps.lmatches.poll()) != null) {
			
			// Get the best minutia of each fingerprint
			Minutia best_minutia_template = templatels.get(lm.b1).getMinutia();
			Minutia best_minutia_input = inputls.get(lm.b2).getMinutia();
			
			// Transform all the LS of both fingerprints using the best minutiae pair.
			for(int i = 0; i < templatels.size(); i++) {
				template_Fg[i] = templatels.get(i).transformMinutia(best_minutia_template);
			}
			for(int i = 0; i < inputls.size(); i++) {
				input_Fg[i] = inputls.get(i).transformMinutia(best_minutia_input);
			}
			
			// Compute "ml" matrix, for all the pairs of transformed minutiae.	
			ml.clear();
			
			for(int i = 0; i < templatels.size(); i++)
				for(int j = 0; j < inputls.size(); j++) {
					
					boolean out = false;
					
					for(int k = 0; k < 3; k++)
						out = out || !(Math.abs(template_Fg[i][k] - input_Fg[j][k]) < LocalStructureJiang.BG[k]);
		
					if(!out)
						try {
							ml.add(new LocalMatch(i, j, 0.5 + 0.5*templatels.get(i).similarity(inputls.get(j))));
						} catch (LSException e) {									
							System.err.println("MatchingReducer.reduce: error when computing the similarity for minutiae (" +
									templatels.get(i).getFpid() + "," + templatels.get(i).getLSid() + ") and (" +
						inputls.get(j).getFpid() + "," + inputls.get(j).getLSid() + ")");
							e.printStackTrace();
						}
					
				}
	
			// Compute the sum of "ml", avoiding to use the same minutia twice.
			double mlsum = 0.0;
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
		
		return maxmlsum/Math.max(templatels.size(), inputls.size());
	}
//	@SuppressWarnings("unchecked")
//	public <T extends PartialScore> PartialScore partialAggregate(PartialScoreKey key, Iterable<T> values, Map<?,?> infomap) throws LSException, IOException {
//		
//		return new PartialScoreJiang((Iterable<PartialScoreJiang>) values);
//	}

	

	public Map<String, List<LocalStructureJiang>> loadInfoFile(Configuration conf) {

		String name = conf.get(Util.MAPFILENAMEPROPERTY, Util.MAPFILEDEFAULTNAME);
    	MapFile.Reader infofile = Util.createMapFileReader(conf, name);
    	
    	Map<String, List<LocalStructureJiang>> infomap = new HashMap<String, List<LocalStructureJiang>>();

		@SuppressWarnings("rawtypes")
		WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(infofile.getKeyClass(), conf);
		
		ArrayWritable value = (new LocalStructureJiang().newArrayWritable());
		
		try {
			while(infofile.next(key, value)) {
				List<LocalStructureJiang> alsj = new ArrayList<LocalStructureJiang>(value.get().length);
				
				for(Writable lsj : value.get())
					alsj.add((LocalStructureJiang) lsj);
				infomap.put(key.toString(), alsj);
			}
		} catch (Exception e) {
			System.err.println("PartialScoreJiang.loadInfoFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}
		
		IOUtils.closeStream(infofile);
		
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

	public PartialScore computePartialScore(LocalStructure ls,
			LocalStructure[] als) {
		return new PartialScoreJiang(ls, als);
	}

//	@Override
//	public PartialScore partialAggregate(PartialScoreKey key,
//			Iterable<PartialScore> values, Map<?, ?> infomap) {
//
//		return new PartialScoreJiang(values);
//	}

	public PartialScore partialAggregateG(PartialScoreKey key,
			Iterable<GenericPSWrapper> values, Map<?, ?> infomap) {

		return new PartialScoreJiang(values);
	}

	public boolean isEmpty() {
//		return (lmatches == null || lmatches.peek() == null || lmatches.peek().sl <= 0);
		
		// All the local structures are needed for the consolidation, so they cannot be discarded
		return false;
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {
		// TODO Auto-generated method stub
		return null;
	}

	public double computeScore(int inputsize) {
		// TODO Auto-generated method stub
		return 0;
	}
}
