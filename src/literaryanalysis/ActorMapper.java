/**
 * Student ID:   D10126532
 * Student Name: John Warde
 * Course Code:  DT230B
 * 
 */

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


public class ActorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

	static long lChunkStartLineNo = -1;
	// Records the Nth line number processed by this instance of the Mapper
	static long lMapperLineNumber = 0;
	static String lastActor = null;
	static long lastActorLineNumber = -1;
	
	long lFileSize = 0;
	long lAverageLineSize = 0;
	boolean useSorting = true;

	public void configure(JobConf job) {
		lAverageLineSize = job.getLong("average-line-length-bytes", 0);
		System.out.println("QWERTY: Average bytes per line is  " + lAverageLineSize);	
	}
	
	@Override
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, LongWritable> output, Reporter reporter)
		throws IOException {
		long lKey = key.get();
		System.out.println(String.format("Processing key = [%d], mapper line number = [%d] ", lKey, lMapperLineNumber));
		lMapperLineNumber++;
		if (0 == value.getLength()) {
			// if there is nothing to process get out, for efficiency
			return;
		}
		String s = value.toString();
		String ActorName;
		String Pairing;
		int compareResult;
		long lEstimatedLineNo = lKey / lAverageLineSize;
		
		String pattern = "^([A-Z]+?)\t";
//		Pattern r = Pattern.compile(pattern, Pattern.MULTILINE);
		Pattern r = Pattern.compile(pattern);
		Matcher m = r.matcher(s);

		while (m.find()) {
			ActorName = m.group(1);
			if (null == lastActor) {
				lastActor = ActorName;
				lastActorLineNumber = lMapperLineNumber;
			} else {
				compareResult = lastActor.compareTo(ActorName);
				if (0 == compareResult) {
					// If this happens the we may have a parsing issue. 
					System.out.println(String.format("WARNING: found same actor name (%s) adjacent, mapper line numbers %d & %d.", ActorName, lastActorLineNumber, lMapperLineNumber));
					continue;
				} else {
					if (useSorting && compareResult > 0) {
						Pairing = String.format("%s|%s", ActorName, lastActor);
					} else {
						Pairing = String.format("%s|%s", lastActor, ActorName);
					}
				}
				lastActor = ActorName;
				lastActorLineNumber = lMapperLineNumber;
				System.out.println(String.format("Pairing: %s", Pairing));
	    		output.collect(new Text(Pairing), new LongWritable(1));
			}
    		System.out.println(String.format("mapper: found %s at estimated line no. %d", ActorName, lEstimatedLineNo));
		}		
	}
	
}
