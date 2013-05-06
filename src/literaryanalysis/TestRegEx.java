package literaryanalysis;

//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

public class TestRegEx {

	/**
	 * @param args
	 * @throws IOException 
	 */
	
/*	
	public static void hidemain(String[] args) throws IOException {
	    BufferedReader reader = new BufferedReader( new FileReader ("PlayContent/RomeoJuliet.txt"));
	    String         line = null;
	    String ActorName;


	    int intLineNo = 0;
	    while( ( line = reader.readLine() ) != null ) {
	    	if (0 == line.length())
	    	{
	    		continue;
	    	}
	    	if (39 < intLineNo) {
	    		// here
	    	}
	    	System.out.println(String.format("%d: %s", intLineNo, line));
			String pattern = "^([A-Z]+?)\t";
			
			Pattern r = Pattern.compile(pattern, Pattern.MULTILINE);
			Matcher m = r.matcher(line);

			while (m.find()) {
				System.out.println("Found value: " + m.group(0) );
				ActorName = m.group(1);
		    	System.out.println(String.format("Found actor name on line %d: %s", intLineNo, ActorName));
			}
			intLineNo++;
	    }
	    reader.close();
	}
*/
}
