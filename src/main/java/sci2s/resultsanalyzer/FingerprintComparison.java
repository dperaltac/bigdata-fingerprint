package sci2s.resultsanalyzer;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class FingerprintComparison implements java.io.Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected String db;
	
	protected String[] types = {
		"nist",
		"sfinge",
		"fvc",
		"mcyt",
		"casiav5",
		"fingerpass",
		"spain",
		"unknown"
	};
	
	protected String[] patterns = {
		"\\/[f|s|F|S][0-9]*(\\.xyt)?$",
		"\\/(Sfinge)?[A-Z][a-z]+[0-9]+_[0-9]+(\\.xyt)?$",
		"\\/[0-9]+_[0-9]+(\\.xyt)?$",
		"\\/(pb|dp)+_[0-9]{4}_[0-9]_[0-9]+(\\.xyt)?$",
		"\\/[0-9]{3}\\/[LR]\\/[0-9]{3}_[LR][0-3]_[0-9]+(\\.xyt)?$",
		"\\/[A-Za-z0-9]+_[0-9]{4}_[0-9]{2}(\\.xyt)?$",
		"\\/[0-9]{5}[A-Z]{4}(RE|V[1-3])[0-9]{3}(_1[12])?(\\.xyt)?$"
	};
	
	public FingerprintComparison() {
		db = "unknown";
	}

	public FingerprintComparison(String database) {
		db = database;
	}
	
	public String [] getTypes() {
		return types;
	}
	
	public String getType(String f) {
	
		Pattern pattern = null;
		Matcher matcher = null;
		
		for(int i = 0; i < patterns.length; i++) {
			pattern = Pattern.compile(patterns[i]);
			
			matcher = pattern.matcher(f);
			
			if(matcher.find())
				return types[i];
		}
		
		return "unknown";
	}

	public boolean genuine(String f1, String f2) {
	
		String type = db, db1=null, db2=null;
		
		if(type.equals("unknown")) {
			db1 = getType(f1);
			db2 = getType(f2);
			
			if(db1.equals(db2))
				type = db1;
			else
			{
// 				System.out.println("Different types for:");
// 				System.out.println("\t" + db1 + "\t" + f1);
// 				System.out.println("\t" + db2 + "\t" + f2);
// 				System.out.println("");
				return false;
			}
		}
			
		if(type.equals("nist"))
			return genuineNIST(f1, f2);
		else if(type.equals("sfinge") || type.equals("fvc"))
			return genuineSfinge(f1, f2);
		else if(type.equals("mcyt"))
			return genuineMCYT(f1, f2);
		else if(type.equals("fingerpass"))
			return genuineFingerPass(f1, f2);
		else if(type.equals("casiav5"))
			return genuineCASIA(f1, f2);
		else if(type.equals("spain"))
			return genuineSpain(f1, f2);
		else
			return false;
	}

	public boolean genuineNIST(String f1, String f2) {
		String objetivo = f1.replaceAll("[f|F|s|S]", "L");
		String token = f2.replaceAll("[f|F|s|S]", "L");

		return(token.equals(objetivo));
	}

	public boolean genuineSfinge(String f1, String f2) {

		String objetivo = f1.substring(f1.lastIndexOf("/")+1, f1.lastIndexOf("_")+1);
		objetivo = objetivo.substring(objetivo.indexOf(".")+1);

		return f2.contains(objetivo);
	}

	public boolean genuineMCYT(String f1, String f2) {
		
		String f1root = f1.substring(f1.lastIndexOf("/")+1, f1.lastIndexOf("."));
		String f2root = f2.substring(f2.lastIndexOf("/")+1, f2.lastIndexOf("."));
	
		StringTokenizer tokens1 = new StringTokenizer (f1root,"_");
		StringTokenizer tokens2 = new StringTokenizer (f2root,"_");

		// We discard the sensor information
		tokens1.nextToken();
		tokens2.nextToken();
		
		// If the person and the finger are the same
		return (tokens1.nextToken().equals(tokens2.nextToken()) &&
		        tokens1.nextToken().equals(tokens2.nextToken()));
	}

	public boolean genuineFingerPass(String f1, String f2) {
		
		String f1root = f1.substring(f1.lastIndexOf("/")+1, f1.lastIndexOf("."));
		String f2root = f2.substring(f2.lastIndexOf("/")+1, f2.lastIndexOf("."));
	
		StringTokenizer tokens1 = new StringTokenizer (f1root,"_");
		StringTokenizer tokens2 = new StringTokenizer (f2root,"_");

		// We discard the sensor information
		tokens1.nextToken();
		tokens2.nextToken();
		
		// If the finger is the same
		return (tokens1.nextToken().equals(tokens2.nextToken()));
	}

	public boolean genuineSpain(String f1, String f2) {
		
		int i1 = f1.lastIndexOf("/")+1;
		int i2 = f2.lastIndexOf("/")+1;
		
		String f1root = f1.substring(i1, i1+7);
		String f2root = f2.substring(i2, i2+7);
		
		return (f1root.equals(f2root));
	}

	public boolean genuineCASIA(String f1, String f2) {

		String objetivo = f1.substring(f1.lastIndexOf("/")+1, f1.lastIndexOf("_")+1);

		return f2.contains(objetivo);
	}
}
