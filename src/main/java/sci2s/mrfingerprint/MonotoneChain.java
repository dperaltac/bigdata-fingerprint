package sci2s.mrfingerprint;
import java.util.ArrayList;
import java.util.Arrays;
 
class Point implements Comparable<Point> {
	int x, y;
 
	public int compareTo(Point p) {
		if (this.x == p.x) {
			return this.y - p.y;
		} else {
			return this.x - p.x;
		}
	}
 
	public String toString() {
		return "("+x + "," + y+")";
	}
 
}

// TODO revisar eficiencia
public class MonotoneChain {
 
	public static long cross(Point O, Point A, Point B) {
		return (A.x - O.x) * (B.y - O.y) - (A.y - O.y) * (B.x - O.x);
	}
 
	public static Point[] convex_hull(Point[] P) {
 
		if (P.length > 1) {
			int n = P.length, k = 0;
			Point[] H = new Point[2 * n];
 
			Arrays.sort(P);
 
			// Build lower hull
			for (int i = 0; i < n; ++i) {
				while (k >= 2 && cross(H[k - 2], H[k - 1], P[i]) <= 0)
					k--;
				H[k++] = P[i];
			}
 
			// Build upper hull
			for (int i = n - 2, t = k + 1; i >= 0; i--) {
				while (k >= t && cross(H[k - 2], H[k - 1], P[i]) <= 0)
					k--;
				H[k++] = P[i];
			}
			if (k > 1) {
				H = Arrays.copyOfRange(H, 0, k - 1); // remove non-hull vertices after k; remove k - 1 which is a duplicate
			}
			return H;
		} else if (P.length <= 1) {
			return P;
		} else{
			return null;
		}
	}
 
	public static Point[] convex_hull(ArrayList<Minutia> minutiae) {
 
		Point[] p = new Point[minutiae.size()];
		
		for (int i = 0; i < p.length; i++) {
			p[i] = new Point();
			p[i].x = minutiae.get(i).getX(); // Read X coordinate 
			p[i].y = minutiae.get(i).getY(); // Read y coordinate
		}
 
		return convex_hull(p).clone();
	}
 
}