package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.common.IOUtils;

public class PartialScoreLSS implements PartialScore {
	
	protected ArrayPrimitiveWritable bestsimilarities;
	protected IntWritable templatesize;

	//TODO It's not necessary to compute NP every time!!! All the PS for the same input fingerprint will have the same partialNP
	
	public static final double MUP = 20; //!< Sigmoid parameter 1 in the computation of n_P
	public static final double TAUP = 0.4; //!< Sigmoid parameter 2 in the computation of n_P
	public static final int MINNP = 4; //!< Minimum number of minutiae in the computation of n_P
	public static final int MAXNP = 12; //!< Maximum number of minutiae in the computation of n_P
	
	public PartialScoreLSS() {

		bestsimilarities = new ArrayPrimitiveWritable();
		templatesize = new IntWritable(0);
		
	}
	
	public PartialScoreLSS(PartialScoreLSS o) {
		
		bestsimilarities = new ArrayPrimitiveWritable(o.bestsimilarities.get());
		templatesize = new IntWritable(o.templatesize.get());
		
	}
	
	public PartialScoreLSS(double [] bs, int ts) {
		
		bestsimilarities = new ArrayPrimitiveWritable(bs);
		templatesize = new IntWritable(ts);
	}
	
	
	// Parameter constructor. Performs the partialAggregateG operation.
	public PartialScoreLSS(Iterable<GenericPSWrapper> values, int np) {

		PriorityQueue<Double> best = new PriorityQueue<Double>(200, Collections.reverseOrder());
		int tam = 0;
		PartialScoreLSS psc;
		
		// Aggregate all similarity values
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSS) ps.get();

			double [] bestsl = (double []) psc.bestsimilarities.get();
			for(double sl : bestsl)
				best.add(sl);

			tam += psc.templatesize.get();
		}
		
		templatesize = new IntWritable(tam);

		double [] npbest = new double[Math.min(np, best.size())];
		for(int i = 0; i < npbest.length; ++i)
			npbest[i] = best.poll();
		
		bestsimilarities = new ArrayPrimitiveWritable(npbest);
	}

	public PartialScoreLSS(LocalStructure ls, LocalStructure [] als) {
		
		// If the cylinder is not valid, no partial score is computed
		if(!((LocalStructureCylinder) ls).isValid()) {

			bestsimilarities = new ArrayPrimitiveWritable();
			templatesize = new IntWritable(0);
			
			return;
		}

		PriorityQueue<Double> gamma = new PriorityQueue<Double>(als.length, Collections.reverseOrder());
		double sl;

		for(LocalStructure ils : als) {

			// If the cylinder is not valid, no partial score is computed
			if(((LocalStructureCylinder) ils).isValid()) {
				try {
					sl = ls.similarity(ils);
					
					if(sl > 0)
						gamma.add(sl);
					
				} catch (LSException e) {
					System.err.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}

		int np = Math.min(computeNP(als.length), gamma.size());
		double [] npbest = new double [np];
		
		for(int i = 0; i < np; ++i)
			npbest[i] = gamma.poll();
		
		bestsimilarities = new ArrayPrimitiveWritable(npbest);
		templatesize = new IntWritable(1);
	}
	
	@Override
	public String toString() {
		return bestsimilarities.toString();
	}

	public void readFields(DataInput in) throws IOException {

		bestsimilarities.readFields(in);
		templatesize.readFields(in);
	}

	public void write(DataOutput out) throws IOException {

		bestsimilarities.write(out);
		templatesize.write(out);
	}

	
	public static int computeNP(int n_A, int n_B)
	{
	  return MINNP + (int)Math.round(Util.psi(Math.min(n_A,n_B), MUP, TAUP*(MAXNP-MINNP)));
	}
	
	public static int computeNP(int n_A)
	{
	  return MINNP + (int)Math.round(Util.psi(n_A, MUP, TAUP*(MAXNP-MINNP)));
	}


	
	public double aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		List<Double> best = new ArrayList<Double>();
		int tam = 0;
		double sum = 0.0;
		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());
		
		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}

		PartialScoreLSS psc;
		
		// Concatenate all similarity values
		for(GenericPSWrapper ps : values) {
			
			psc = (PartialScoreLSS) ps.get();

			double [] bestsl = (double []) psc.bestsimilarities.get();
			for(double sl : bestsl)
				best.add(sl);

			tam += psc.templatesize.get();
		}
		
		int np = computeNP(inputsize, tam);

		best = com.google.common.collect.Ordering.natural().greatestOf(best, np);
		
		for(Double x : best)
			sum += x;
		
		return sum/np;

	}

//	public PartialScore partialAggregate(PartialScoreKey key, Iterable<PartialScore> values, Map<?,?> infomap) {
//
//		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());
//		
//		if(inputsize == null) {
//			System.err.println("No infomap value found for key " + key.getFpidInput());
//			inputsize = 50;
//		}
//
//		return new PartialScoreLSS(values, computeNP(inputsize));
//	}


	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());
		
		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}

		return new PartialScoreLSS(values, computeNP(inputsize));
	}
	

	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {

		String name = conf.get(Util.INFOFILENAMEPROPERTY, Util.INFOFILEDEFAULTNAME);
    	MapFile.Writer infofile = Util.createMapFileWriter(conf, name, Text.class, IntWritable.class);
    	
    	Arrays.sort(inputls, new Comparator<LocalStructure[]>() {
 		   public int compare(LocalStructure [] als1, LocalStructure [] als2) {
 		      return als1[0].fpid.compareTo(als2[0].fpid);
 		   }
 		});

		for(LocalStructure [] ails : inputls) {
	    	String fpid = ails[0].fpid;

		    try {
		    	infofile.append(new Text(fpid), new IntWritable(ails.length));
			} catch (IOException e) {
				System.err.println("PartialScoreCylinder.saveInfoFile: unable to save fingerprint "
						+ fpid + " in MapFile " + name + ": " + e.getMessage());
				e.printStackTrace();
			}
		}
		
		IOUtils.closeStream(infofile);
	}
	
	public Map<String, Integer> loadInfoFile(Configuration conf) {

		String name = conf.get(Util.INFOFILENAMEPROPERTY, Util.INFOFILEDEFAULTNAME);
    	MapFile.Reader infofile = Util.createMapFileReader(conf, name);
    	
    	Map<String, Integer> infomap = new HashMap<String,Integer>();

		Text key = new Text();
		IntWritable value = new IntWritable();
		
		try {
			while(infofile.next(key, value)) {
				infomap.put(key.toString(), value.get());
			}
		} catch (Exception e) {
			System.err.println("PartialScoreCylinder.loadInfoFile: unable to read fingerprint "
					+ key + " in MapFile " + name + ": " + e.getMessage());
			e.printStackTrace();
		}
		
		IOUtils.closeStream(infofile);
		
		return infomap;
	}
	
	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureCylinder.class);
	}

	public PartialScore computePartialScore(LocalStructure ls, LocalStructure[] als) {
		return new PartialScoreLSS(ls, als);
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public boolean isEmpty() {
		double [] bs = (double[]) bestsimilarities.get();
		return (bs == null || bs.length == 0 || bs[0] <= 0);
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {

		PartialScoreLSS psc = (PartialScoreLSS) ps;
		PartialScoreLSS result = new PartialScoreLSS();

		PriorityQueue<Double> best = new PriorityQueue<Double>(200, Collections.reverseOrder());

		double [] bestsl = (double []) psc.bestsimilarities.get();
		for(double sl : bestsl)
			best.add(sl);

		bestsl = (double []) bestsimilarities.get();
		for(double sl : bestsl)
			best.add(sl);
		
		result.templatesize = new IntWritable(psc.templatesize.get() + templatesize.get());

		double [] npbest = new double[Math.max(bestsl.length, best.size())];
		for(int i = 0; i < npbest.length; ++i)
			npbest[i] = best.poll();
		
		result.bestsimilarities = new ArrayPrimitiveWritable(npbest);
		
		return result;
	}

	public void aggregateSingleValue(double value) {

		double [] bestsl = (double []) bestsimilarities.get();
		
		int minpos = Util.minPosition(bestsl);

		if(bestsl[minpos] < value) {
			bestsl[minpos] = value;
			bestsimilarities.set(bestsl);
		}
	}

	public double computeScore(int inputsize) {
		
		int np = computeNP(inputsize, templatesize.get());
		double sum = 0.0;

		double [] best = (double[]) bestsimilarities.get();
		
		np = Math.min(np, best.length);
		
		if(np == 0)
			return 0;
		
		for(int i = 0; i < np; i++)
			sum += best[i];
		
		return sum/np;

	}
}
