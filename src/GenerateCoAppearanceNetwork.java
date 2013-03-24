import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
public class GenerateCoAppearanceNetwork extends Configured implements Tool {
	public static void main(String[] args) throws IOException {
		
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), new GenerateCoAppearanceNetwork(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(result);
	}
	

	@Override
	public int run(String[] args) throws Exception {		
		Configuration conf = getConf();
		
		JobConf job = new JobConf(conf, GenerateCoAppearanceNetwork.class);
		job.setJobName("GenerateCoAppearanceNetwork");
 
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(SumReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		JobClient.runJob(job);
		return 0;
	}

}
