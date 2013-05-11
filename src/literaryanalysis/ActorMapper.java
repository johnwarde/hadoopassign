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

	
	private long	lMapperLineNumber 	= 0;    // Records the Nth line number processed by this instance of the Mapper
	private Pattern	ActorRegEx 			= null; // Regular expression pattern for matching actor names
	private String	lastActor 			= null; // Last matched actor name
	private long	lastActorLineNumber = -1;   // Where within the physical HDFS block it was matched
	
	// Corresponding command line values
	private long 	lMaxLineDiff		= -1;
	private boolean bUseSorting		 	= true;
	private boolean bMatchMinorActors = false;

	public void configure(JobConf job) {
		// Retrieve the command line values
		lMaxLineDiff 		= job.getLong(ResourceNames.MATCH_WITHIN_N_LINES_PARAM, -1);
		bUseSorting 		= job.getBoolean(ResourceNames.SORT_PAIRS_PARAM, true);
		bMatchMinorActors	= job.getBoolean(ResourceNames.MATCH_MINOR_ACTORS_PARAM, false);
		// Compile the Regular Expression only one during the Mapper instance to save processing time,
		// actor names are all UPPER CASE, minor actors include lower case characters name i.e. "Nurse"
		ActorRegEx 			= bMatchMinorActors ? Pattern.compile("^([A-Za-z ]+?)\t") : Pattern.compile("^([A-Z ]+?)\t");
	}
	
	@Override
	public void map(LongWritable key, Text value, 
			OutputCollector<Text, LongWritable> output, Reporter reporter)
		throws IOException {
		//System.out.println(String.format("Processing key = [%d], mapper line number = [%d] ", lKey, lMapperLineNumber));
		lMapperLineNumber++;
		if (0 == value.getLength()) {
			// If there is nothing to match then get out, for efficiency
			return;
		}
		String s = value.toString();
		String ActorName;
		String Pairing = null;
		int compareResult;

		Matcher m = ActorRegEx.matcher(s);

		while (m.find()) {
			ActorName = m.group(1);
			if (ActorName.contains("SCENE")) {
				// Not an actor name, so skip
				return;
			}
			if (null == lastActor) {
				// Recording first instance of an actor name
				lastActor 			= ActorName;
				lastActorLineNumber = lMapperLineNumber;
				// No more processing to do for this map() call.
				return;
			}
			compareResult = lastActor.compareTo(ActorName);
			if (0 == compareResult) {  // Actor name is identical to the last recorded
				// If this happens then we may have a parsing issue.
				// If we are matching minor actors (e.g. skipping what the Nurse says) then 
				// the same upper case actor name will legitimately be found one after another.
				if (bMatchMinorActors) {
					// We are matching minor actor names and 
					// still found an repeating adjacent actor name,
					// we may have a parsing problem.
					System.out.println(String.format("WARNING: found same actor name (%s) adjacent, mapper line numbers %d & %d.", ActorName, lastActorLineNumber, lMapperLineNumber));
					reporter.incrCounter("Mapper", ResourceNames.SAME_ACTOR_NAME_ADJACENT_COUNTER, 1);
				}
				lastActor 			= ActorName;
				lastActorLineNumber = lMapperLineNumber;
				return;
			}
			if (-1 == lMaxLineDiff ||   // If the line distance does not matter OR  
			   (lMapperLineNumber - lastActorLineNumber) < lMaxLineDiff) {  //	it does and the line distance is within range then
				// Record a pairing
				if (bUseSorting && compareResult > 0) {
					Pairing = String.format("%s|%s", ActorName, lastActor);
				} else {
					Pairing = String.format("%s|%s", lastActor, ActorName);
				}
				System.out.println(String.format("Pairing: (%s), lines (%d, %d)", Pairing, lastActorLineNumber, lMapperLineNumber));
	    		output.collect(new Text(Pairing), new LongWritable(1));
				lastActor 			= ActorName;
				lastActorLineNumber = lMapperLineNumber;
			}
		}		
	}
	
}
