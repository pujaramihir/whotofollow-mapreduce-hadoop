import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 */

/**
 * @author Mihir Pujara & Mohit Pujara
 *
 */
public class whotofollow {
	/**
	 * Class for first map task
	 * 
	 * @author Mihir Pujara
	 *
	 */
	public class MapperFirst extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
			
			// Key is ignored as.i""t only stores the offset of the line in the text file
            StringTokenizer st = new StringTokenizer(values.toString());
            
            System.out.println(st.toString());
			
		}
	}
	
	
	/**
	 * Class for first reduce task
	 * 
	 * @author Mihir Pujara
	 *
	 */
	public class ReducerFirst extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
		}
	}
	
	
	public static void main(String[] args) {
		// create configuration 
		Configuration conf = new Configuration();
		
		// create job for first map reduce task
        Job firstJob;
		try 
		{
			firstJob = Job.getInstance(conf, "who to follow first job");
	        firstJob.setJarByClass(whotofollow.class);
	        
	        // set map class reference
	        firstJob.setMapperClass(MapperFirst.class);
	        
	        // set reduce class reference 
	        firstJob.setReducerClass(ReducerFirst.class);
	        
	        // set output class 
	        firstJob.setOutputKeyClass(IntWritable.class);
	        
	        // set output value class
	        firstJob.setOutputValueClass(IntWritable.class);
	        
	        // set input and output details
	        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
	        FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));
	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
