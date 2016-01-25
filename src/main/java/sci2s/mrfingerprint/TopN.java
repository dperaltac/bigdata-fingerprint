package sci2s.mrfingerprint;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class TopN<E extends Comparable<? super E>> implements Iterable<E>, Collection<E> {

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
	
	public TopN(TopN<E> lmatches) {
		set = new TreeSet<E>(lmatches.set);
		maxsize = lmatches.maxsize;
	}

	public boolean add(E elem) {
		if(set.size() < maxsize)
			return set.add(elem);
		else if(set.comparator().compare(set.last(), elem) > 0) {
			set.pollLast();
			return set.add(elem);
		}
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
	
	public int size() {
		return set.size();
	}
	
	public void truncate(int newsize) {
		for(int i = set.size()-1; i >= newsize; i--)
			set.pollLast();
	}
	
	public void setMax(int newsize) {
		maxsize = newsize;
		
		if(maxsize < set.size())
			truncate(maxsize);
	}
	
	public int getMax() {
		return maxsize;
	}
	
	public E poll() {
		return set.pollFirst();
	}
	
	public E first() {
		return set.first();
	}

	public void clear() {
		set.clear();
	}

	public boolean contains(Object o) {
		return set.contains(o);
	}

	public boolean containsAll(Collection<?> c) {
		return set.containsAll(c);
	}

	public boolean isEmpty() {
		return set.isEmpty();
	}

	public boolean remove(Object o) {
		return set.remove(o);
	}

	public boolean removeAll(Collection<?> c) {
		return set.removeAll(c);
	}

	public boolean retainAll(Collection<?> c) {
		return set.retainAll(c);
	}

	public <T> T[] toArray(T[] a) {
		return set.toArray(a);
	}
	
	public Object[] toArray() {
		return set.toArray();
	}
	
	@Override
	public String toString() {
		String res = new String();
		
		for(E elem : set)
			res += elem.toString() + ',';
		
		return res;
	}

}