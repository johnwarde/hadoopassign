package literaryanalysis;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

//		File inFile = new File(args[0]);
//		conf.setLong("file.size", inFile.length());
//		inFile = null;
		
		JobConf job = new JobConf(conf, GenerateCoAppearanceNetwork.class);
		job.setJobName("GenerateCoAppearanceNetwork");
		
		//job.setMapperClass(MultipleLocisMapper.class);
		//job.setMapperClass(MatchLocisMapper.class);
		//job.setReducerClass(SumLocisReducer.class);
		job.setMapperClass(ActorMapper.class);

		//job.setMapOutputKeyClass(LongWritable.class);
		//job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//job.setInputFormat(NLineInputFormat.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileOutputFormat.setOutputPath(job, new Path("MultipleLocisResults"));

		JobClient.runJob(job);
		
/*		
		JobConf job2 = new JobConf(conf, GenerateCoAppearanceNetwork.class);
		job2.setJobName("MatchLocisMapper");
 
		//job2.setMapperClass(MatchLocisMapper.class);
		//job2.setReducerClass(SumLocisReducer.class);
		
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		JobClient.runJob(job2);
*/		
		
		return 0;
	}

}
