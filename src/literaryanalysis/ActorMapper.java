package literaryanalysis;
import java.io.IOException;
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

public class ActorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

	static long lChunkStartLineNo = -1;
	static String lastActor = null;
	
	long lFileSize = 0;
	long lAverageLineSize = 0;
	boolean useSorting = true;

	public void configure(JobConf job) {
		//lFileSize = job.getLong("file.size", 0);
		lAverageLineSize = job.getLong("average-line-length-bytes", 0);
		System.out.println("QWERTY: Average bytes per line is  " + lAverageLineSize);	
	}
	
	@Override
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, LongWritable> output, Reporter reporter)
		throws IOException {
		if (0 == value.getLength()) {
			// if there is nothing to process get out, for efficiency
			return;
		}
		String s = value.toString();
		String ActorName;
		String Pairing;
		int compareResult;
		long lEstimatedLineNo = key.get() / lAverageLineSize;

		//System.out.println(this.getClass().toString() + " called with string length : " + s.length());
		
		String pattern = "^([A-Z]+?)\t";	
		Pattern r = Pattern.compile(pattern, Pattern.MULTILINE);
		Matcher m = r.matcher(s);

		while (m.find()) {
			ActorName = m.group(1);
			if (null == lastActor) {
				lastActor = ActorName;
			} else {
				compareResult = lastActor.compareTo(ActorName);
				if (0 == compareResult) {
					// Same actor name, we don't want to record this
					continue;
				} else {
					if (useSorting && compareResult > 0) {
						Pairing = String.format("%s|%s", ActorName, lastActor);
					} else {
						Pairing = String.format("%s|%s", lastActor, ActorName);
					}
				}
				System.out.println(String.format("pairing: %s", Pairing));
	    		output.collect(new Text(Pairing), new LongWritable(1));
			}
    		System.out.println(String.format("mapper: found %s at estimated line no. %d", ActorName, lEstimatedLineNo));
		}		
	}
	
}
