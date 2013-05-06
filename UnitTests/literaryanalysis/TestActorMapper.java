/**
 * 
 */
package literaryanalysis;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

/**
 * @author soc
 *
 */
public class TestActorMapper extends TestCase {
	private ActorMapper mapper;
	
//	private String strContent = new String("GREGORY	They must take it in sense that feel it.\r\n\r\n" +
//									"SAMPSON	Me they shall feel while I am able to stand: andr\n" +
//									"	'tis known I am a pretty piece of flesh.r\n");
	
	/**
	 * @throws java.lang.Exception
	 */
	protected static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	protected static void tearDownAfterClass() throws Exception {
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf, ActorMapper.class);		
		job.setLong("file.size", 444);
		job.setLong("average-line-length-bytes", 240);
		job.setLong("no-of-appearance-lines", 3);
		mapper = new ActorMapper();
		mapper.configure(job);
//		OutputCollector<Text, LongWritable> output = new OutputCollector<Text, LongWritable>(); 
//		Reporter reporter = new Reporter();
//		LongWritable key = new LongWritable(0);
//		mapper.map(key, strContent, output, reporter);	
	}

	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	protected void tearDown() throws Exception {
		super.tearDown();
		mapper = null;
	}

}
