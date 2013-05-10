/**
 * Student ID:   D10126532
 * Student Name: John Warde
 * Course Code:  DT230B
 * 
 */

package literaryanalysis;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class GenerateCoAppearanceNetwork extends Configured implements Tool {
	public static void main(String[] args) throws IOException {
		int result = 0;
		try {
			// Set-up the Job Run (and configuration) using this class and the arguments from the command line
			result = ToolRunner.run(new Configuration(), new GenerateCoAppearanceNetwork(), args);
		} catch (Exception e) {
			System.out.println("ERROR: executing ToolRunner.run()");
			e.printStackTrace();
		}
		System.exit(result);
	}
	

	@Override
	public int run(String[] args) throws Exception {
		// Get current configuration
		Configuration conf = getConf();
		
		// Create a new Hadoop Job using this class
		JobConf job = new JobConf(conf, GenerateCoAppearanceNetwork.class);
		// Set the job name, appears in the Job Tracker web interface
		job.setJobName("GenerateCoAppearanceNetwork");
		
		// Set the Mapper and Reducer for this job
		job.setMapperClass(ActorMapper.class);
		job.setReducerClass(PairingReducer.class);
		
		// Give the option of using the combiner for 
		// benchmarking the different command line options;
		// defaulting to true for best performance in production jobs
		boolean bUseCombiner = job.getBoolean("use-combiner", true);
		System.out.println("Using Combiner? " + (bUseCombiner ? "Yes" : "No"));
		if (bUseCombiner) {
			// Use the same code as the reducer for the combiner, 
			// the combiner will execute on the same node as the 
			// mapper and should reduce network traffic between the 
			// mapper and reducer plus the work the reducer has to do.
			job.setCombinerClass(PairingReducer.class);
		}
		
		// Set the Key and Value types for the Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// Set the Key and Value types for the output of the
		// Reducer and the job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// Set the input and output filename for the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Run the Hadoop Job!
		JobClient.runJob(job);
				
		return 0;
	}

}
