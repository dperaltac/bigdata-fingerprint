package sci2s.mrfingerprint;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class PartialScoreLSSR implements PartialScore {
	
	static protected class LocalMatch implements Comparable<LocalMatch>, Writable {

		protected Minutia b1min;
		protected Minutia b2min;
		protected double sl;

		public LocalMatch() {
			b1min = new Minutia();
			b2min = new Minutia();
			sl = 0.0;
		}
		
		public LocalMatch(LocalMatch o) {
			b1min = new Minutia(o.b1min);
			b2min = new Minutia(o.b2min);
			sl = o.sl;
		}
		
		public LocalMatch(Minutia b1, Minutia b2, double sl) {
			this.b1min = b1;
			this.b2min = b2;
			this.sl = sl;
		}

		public int compareTo(LocalMatch arg0) {
			return Double.compare(sl, arg0.sl);
		}

		public void readFields(DataInput arg0) throws IOException {
			b1min.readFields(arg0);
			b2min.readFields(arg0);
			sl = arg0.readDouble();
		}

		public void write(DataOutput arg0) throws IOException {
			b1min.write(arg0);
			b2min.write(arg0);
			arg0.writeDouble(sl);
		}
		
		protected final static Comparator<LocalMatch> inverted_comparator = new Comparator<LocalMatch>(){
					public int compare(LocalMatch arg0, LocalMatch arg1) {
						int res = arg1.compareTo(arg0);
						
						if(res == 0) {
							res = Integer.compare(arg0.b1min.getIndex(),arg1.b1min.getIndex());
							
							if(res == 0)
								return Integer.compare(arg0.b2min.getIndex(),arg1.b2min.getIndex());
						}
							
						return res;
					}
				};
				
		public static Comparator<LocalMatch> invertedComparator() {
			return inverted_comparator;
		}
	}
	
	static protected class LocalMatchArray extends ArrayWritable {
		public LocalMatchArray() {
			super(LocalMatch.class);
		}

		public LocalMatchArray(LocalMatch [] values) {
			super(LocalMatch.class, values);
		}
	}
	
	protected LocalMatch [] lmatches;
	protected IntWritable templatesize;
	
	public static final int NREL = 5;
	public static final double WR = 0.5; //!< Weight parameter in Relaxation computation
	public static final double MUP1 = 5; //!< Sigmoid parameter 1 in the computation of d_1 relaxation
	public static final double TAUP1 = -1.6; //!< Sigmoid parameter 2 in the computation of d_1 relaxation
	public static final double MUP2 = 0.261799387799; //!< Sigmoid parameter 1 in the computation of d_2 relaxation
	public static final double TAUP2 = -30; //!< Sigmoid parameter 2 in the computation of d_2 relaxation
	public static final double MUP3 = 0.261799387799; //!< Sigmoid parameter 1 in the computation of d_3 relaxation
	public static final double TAUP3 = -30; //!< Sigmoid parameter 2 in the computation of d_3 relaxation
		
	public PartialScoreLSSR() {

		templatesize = new IntWritable(0);
		lmatches = new LocalMatch[0];
	}
	
	public PartialScoreLSSR(PartialScoreLSSR o) {

		templatesize = new IntWritable(o.templatesize.get());
		lmatches = Util.arraycopy(o.lmatches);
	}
	
	// Parameter constructor. Performs the partialAggregate operation.
	public PartialScoreLSSR(Iterable<GenericPSWrapper> values, int nr) {

		PriorityQueue<LocalMatch> heap = new PriorityQueue<LocalMatch>(nr, LocalMatch.invertedComparator());
		PartialScoreLSSR psc;
				
		int tam = 0;
		
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSSR) ps.get();
			
			for(LocalMatch lm : psc.lmatches)
				heap.add(lm);
			
			tam +=  psc.templatesize.get();
		}

		lmatches = new LocalMatch[Math.min(nr, heap.size())];
		for(int i = 0; i < lmatches.length; i++)
			lmatches[i] = heap.poll();
		
		templatesize = new IntWritable(tam);
	}
	
	// Parameter constructor. Performs the partialAggregate operation.
	public PartialScoreLSSR(Iterable<GenericPSWrapper> values) {

		PriorityQueue<LocalMatch> heap = new PriorityQueue<LocalMatch>(100, LocalMatch.invertedComparator());
		PartialScoreLSSR psc;
				
		int tam = 0;
		
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSSR) ps.get();
			
			for(LocalMatch lm : psc.lmatches)
				heap.add(lm);
			
			tam +=  psc.templatesize.get();
		}

		lmatches = new LocalMatch[heap.size()];
		for(int i = 0; i < lmatches.length; i++)
			lmatches[i] = heap.poll();

		templatesize = new IntWritable(tam);
	}

	public PartialScoreLSSR (LocalStructure ls, LocalStructure[] als) {
		double sl;
		
		// If the cylinder is not valid, no partial score is computed
		if(!((LocalStructureCylinder) ls).isValid())
			return;

		PriorityQueue<LocalMatch> heap = new PriorityQueue<LocalMatch>(als.length, LocalMatch.invertedComparator());

		// At this point we only have one template minutia
		Minutia b1 = ((LocalStructureCylinder) ls).getMinutia();
		Minutia b2;

		for(LocalStructure ils : als) {			
			LocalStructureCylinder ilsc = (LocalStructureCylinder) ls;

			// If the cylinder is not valid, no partial score is computed
			if(ilsc.isValid()) {
				try {
					sl = ls.similarity(ils);
					
					if(sl > 0) {
						b2 = ((LocalStructureCylinder) ils).getMinutia();
						heap.add(new LocalMatch(b1, b2, sl));
					}
				} catch (LSException e) {
					System.err.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}

		lmatches = new LocalMatch[heap.size()];
		for(int i = 0; i < lmatches.length; i++)
			lmatches[i] = heap.poll();

		templatesize = new IntWritable(1);
	}

	public void readFields(DataInput in) throws IOException {

		templatesize.readFields(in);

		// Read the local matches
		LocalMatchArray auxaw = new LocalMatchArray(lmatches);
		auxaw.readFields(in);
		lmatches = (LocalMatch[]) auxaw.toArray();
	}

	public void write(DataOutput out) throws IOException {
		
		templatesize.write(out);

		LocalMatchArray auxaw = new LocalMatchArray(lmatches);
		
		auxaw.write(out);
	}
	
	public static double rho(Minutia t_a, Minutia t_b, Minutia k_a, Minutia k_b) {
		
		double d1, d2, d3;

		d1 = Math.abs(t_a.getDistance(k_a.getX(),k_a.getY()) - t_b.getDistance(k_b.getX(),k_b.getY()));
		d2 = Math.abs(Util.dFiMCC(Util.dFiMCC(t_a.getrT(),k_a.getrT()),Util.dFiMCC(t_b.getrT(),k_b.getrT())));
		d3 = Math.abs(Util.dFiMCC(dR(t_a,k_a),dR(t_b,k_b)));
	
		return Util.psi(d1,MUP1,TAUP1) * Util.psi(d2,MUP2,TAUP2) * Util.psi(d3,MUP3,TAUP3);
	}
	
	public static double dR(Minutia m1, Minutia m2)
	{
	    return Util.dFiMCC(m1.getrT(), Math.atan2(m1.getY()-m2.getY(), m2.getX()-m1.getX()));
	}

	public double aggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {

		int tam = 0;

		Integer inputsize = (Integer) infomap.get(key.getFpidInput().toString());
		
		if(inputsize == null) {
			System.err.println("No infomap value found for key " + key.getFpidInput());
			inputsize = 50;
		}
		
		PriorityQueue<LocalMatch> bestmatches = new PriorityQueue<LocalMatch>(inputsize*50, LocalMatch.invertedComparator());

		PartialScoreLSSR psc;
		
		// Concatenate all similarity values
		for(GenericPSWrapper ps : values) {
			psc = (PartialScoreLSSR) ps.get();
			
			for(LocalMatch lm : psc.lmatches)
				bestmatches.add(lm);
			
			tam +=  psc.templatesize.get();
		}

		// Extract the best nr pairs (LSS consolidation)
		int nr = Math.min(Math.min(inputsize, tam), bestmatches.size());
		int np = computeNP(inputsize, tam);
		
		LocalMatch[] bestlm = new LocalMatch[nr];
		double [] bestscores = new double[nr];
		
		for(int i = 0; i < nr; i++)
		{
			bestlm[i] = bestmatches.poll();
			bestscores[i] = bestlm[i].sl;
		}
		
		return consolidation(bestscores, bestlm, np);
	}
	
	public PartialScore partialAggregateG(PartialScoreKey key, Iterable<GenericPSWrapper> values, Map<?,?> infomap) {
		
		Integer nr = (Integer) infomap.get(key.getFpidInput().toString());
		return new PartialScoreLSSR(values, nr);
	}
	
	public static int computeNP(int n_A, int n_B)
	{
		return PartialScoreLSS.computeNP(n_A, n_B);
	}
	
	public static int computeNP(int n_A)
	{
		return PartialScoreLSS.computeNP(n_A);
	}
	
	public void saveInfoFile(LocalStructure[][] inputls, Configuration conf) {
		(new PartialScoreLSS()).saveInfoFile(inputls, conf);
	}

	public Map<String, Integer> loadInfoFile(Configuration conf) {
		return (new PartialScoreLSS()).loadInfoFile(conf);
	}
	
	public <T extends LocalStructure> boolean isCompatibleLS(Class<T> lsclass) {
		return (lsclass == LocalStructureCylinder.class);
	}

	public PartialScore computePartialScore(LocalStructure ls, LocalStructure[] als) {
		return new PartialScoreLSSR(ls, als);
	}

	public Map<?, ?> loadCombinerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public Map<?, ?> loadReducerInfoFile(Configuration conf) {
		return loadInfoFile(conf);
	}

	public boolean isEmpty() {
		return (lmatches == null || lmatches.length == 0 || lmatches[0].sl <= 0);
	}

	public PartialScore aggregateSinglePS(PartialScore ps) {

		PartialScoreLSSR psc = (PartialScoreLSSR) ps;
		PartialScoreLSSR result = new PartialScoreLSSR();
		
		int totallmatches = psc.lmatches.length + lmatches.length;  
		
		if(totallmatches > 0) {
			PriorityQueue<LocalMatch> heap = new PriorityQueue<LocalMatch>(psc.lmatches.length + lmatches.length,
					LocalMatch.invertedComparator());
						
			for(LocalMatch lm : psc.lmatches)
				heap.add(lm);
			for(LocalMatch lm : lmatches)
				heap.add(lm);

			result.lmatches = new LocalMatch[heap.size()];
			for(int i = 0; i < result.lmatches.length; i++)
				result.lmatches[i] = heap.poll();
		}
		else {
			result.lmatches = new LocalMatch[0];
		}
	
		result.templatesize = new IntWritable(psc.templatesize.get() + templatesize.get());
		
		return result;
	}

	public double computeScore(int inputsize) {
		
		PriorityQueue<LocalMatch> bestmatches = new PriorityQueue<LocalMatch>(lmatches.length, LocalMatch.invertedComparator());
		
		// Concatenate all similarity values
		for(LocalMatch lm : lmatches)
			bestmatches.add(lm);

		// Extract the best nr pairs (LSS consolidation)
		int nr = Math.min(Math.min(inputsize, templatesize.get()), lmatches.length);
		int np = computeNP(inputsize, templatesize.get());
		
		double [] bestscores = new double[nr];
		LocalMatch[] bestlm = new LocalMatch[nr];
		
		for(int i = 0; i < nr; i++)
		{
			bestlm[i] = bestmatches.poll();
			bestscores[i] = bestlm[i].sl;
		}
		
		return consolidation(bestscores, bestlm, np);
	}
	
	protected double consolidation(double [] bestscores, LocalMatch[] bestlm, int np)
	{
		double [] lambdaT = Arrays.copyOf(bestscores, bestscores.length);
		double [] lambdaT1 = new double[lambdaT.length];
		
		int nr = bestscores.length;

		final double LAMBDAWEIGHT = (1.0-WR)/(nr-1.0);
		
		double [][] rhotab = new double[nr][nr];
		
		double sum = 0.0;
		
		for (int j=0; j<nr; j++)
			for (int k=0; k<nr; k++)
				if (k!=j)
					rhotab[j][k] = rho(bestlm[j].b1min,bestlm[j].b2min, bestlm[k].b1min,bestlm[k].b2min);
				else
					rhotab[j][k] = 0;
		
		// Apply relaxation iterations
		for (int i=0; i<NREL; i++)
		{
			double [] tmp = lambdaT;
			lambdaT = lambdaT1;
			lambdaT1 = tmp;

			for (int j=0; j<nr; j++)
			{
				sum = 0.0;
				for (int k=0; k<nr; k++)
					sum += rhotab[j][k] * lambdaT1[k];
				
				lambdaT[j] = WR*lambdaT1[j] + LAMBDAWEIGHT*sum;
			}
		}

		double [] efficiency = new double[nr];

		for (int i=0; i<nr; i++)
			efficiency[i] = lambdaT[i] / bestscores[i];
		
		Integer [] besteffidx = Util.sortIndexes(efficiency);

		sum = 0.0;

		for (int i=0; i<np; i++)
			sum += lambdaT[besteffidx[nr-i-1]];
		
		return sum/np;
	}
}
