/**
 * Student ID:   D10126532
 * Student Name: John Warde
 * Course Code:  DT230B
 * 
 */

package literaryanalysis;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PairingReducer extends MapReduceBase implements 
    Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterator<LongWritable> values, 
                OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        
            // Used to record the number of times actors appear together
            int weight = 0;

            // Cycle though all the value associated with this key
            while (values.hasNext()) {
                LongWritable value = values.next();
                weight += value.get();
            }
            // Prepare the data in the format that Gephi expects.
            String strOutputKey = (key.toString().replace('|', '\t'));
            strOutputKey = strOutputKey.concat('\t' + "undirected");
            Text outputKey = new Text(strOutputKey);
            // Then emit the weight to indicate the strength of the co-appearance relationship.
            output.collect(outputKey, new LongWritable(weight));
            // Unfortunately, when the output CSV file is viewed in HDFS (or outside)
            // "undirected" appears twice in the file for each line, but this does
            // not happen when you view the Hadoop logs after job completion (see next line of code)
            // Workaround automation was created to post process this data
            // for use in Gephi, see post-process.sh
            
            // Record in log
            System.out.println(String.format("emit: %s %d", strOutputKey, weight));
        }
}

