package literaryanalysis;
import java.io.IOException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Student ID:   D10126532
 * Student Name: John Warde
 * Course Code:  DT230B
 * 
 */

/**
 * @author soc
 *
 */
public class MultipleLocisMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

	int noOfAppearanceLines;

	public void configure(JobConf job) {
		noOfAppearanceLines = job.getInt("no-of-appearance-lines", 3);
		System.out.println("QWERTY: Number of Appearance line is " + noOfAppearanceLines);	
	}
	
	@Override
	public void map(LongWritable key, Text value, 
			OutputCollector<LongWritable, Text> output, Reporter reporter)
		throws IOException {
		
		String s = value.toString();
		String ActorName;
		long startLine;
		long lineNo = key.get();
//		long dmy = reporter.getInputSplit().getLength();

		long uuid = UUID.randomUUID().getLeastSignificantBits();

		System.out.println(this.getClass().toString() + " called with string length : " + s.length());
		
		String pattern = "^([A-Z]+?)(\t)(.*)";		
		Pattern r = Pattern.compile(pattern, Pattern.MULTILINE);
		Matcher m = r.matcher(s);

//		if (m.find( )) {
		while (m.find()) {
			System.out.println("Found value: " + m.group(0) );
			System.out.println("Found value: " + m.group(1) );
			System.out.println("Found value: " + m.group(2) );
			ActorName = m.group(1);
			startLine = lineNo - noOfAppearanceLines + 1;
			for (long i = startLine; i < (lineNo + noOfAppearanceLines); i++) {
				output.collect(new LongWritable(i), new Text(ActorName + "|" + uuid));				
			}	
//		} else {
//			System.out.println("No actor found on line " + key);
		}		
	
		System.out.println("QWERTY: A mapper has just finished processing a line!");
	}
	
}
