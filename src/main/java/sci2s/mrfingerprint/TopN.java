package sci2s.mrfingerprint;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class TopN<E extends Comparable<? super E>> implements Iterable<E> {

	protected TreeSet<E> set;
	protected int maxsize;

	public TopN() {
		set = new TreeSet<E>(Collections.reverseOrder());
		maxsize = 1;
	}
	
	public TopN(int length) {
		set = new TreeSet<E>(Collections.reverseOrder());
		maxsize = length;
	}
	
	public TopN(E[] elems) {
		this(elems.length);
		
		for(E e : elems)
			set.add(e);
	}
	
	public TopN(E[] elems, int length) {
		this(length);
		
		for(E e : elems)
			add(e);
	}
	
	public TopN(Collection<? extends E> elems) {
		set = new TreeSet<E>(elems);
		maxsize = set.size();
	}
	
	public boolean add(E elem) {
		if(set.size() < maxsize || set.comparator().compare(set.last(), elem) > 0)
			return set.add(elem);
		else
			return false;
	}
	
	public SortedSet<E> get() {
		return set;
	}
	
	public boolean addAll(Collection<? extends E> c) {
		boolean added = false;
		
		for(E e : c)
			added = add(e) || added;
		
		return added;
	}
	
	public boolean addAll(E[] c) {
		boolean added = false;
		
		for(E e : c)
			added = add(e) || added;
		
		return added;
	}
	
	public static <E extends Comparable<? super E>> E[] getTopN(E[] arr, int n) {
		
		if(arr.length <= n)
			return arr;
		
		Arrays.sort(arr, Collections.reverseOrder());
		return Arrays.copyOf(arr, n);
	}
	
	public Iterator<E> iterator() {
		return set.iterator();
	}
	
	public Object[] toArray() {
		return set.toArray();
	}
	
	public E[] toArray(E[] c) {
		return set.toArray(c);
	}
	
	public int size() {
		return set.size();
	}
	
	public void truncate(int newsize) {
		for(int i = set.size()-1; i >= newsize; i--)
			set.pollLast();
	}
	
	public E poll() {
		return set.pollFirst();
	}

}