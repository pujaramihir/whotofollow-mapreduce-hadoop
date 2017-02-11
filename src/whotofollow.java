import java.io.IOException;
import java.util.Iterator;
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
            
            // user and follow will be the elements in the emitted pairs.
            IntWritable user = new IntWritable();
            IntWritable follow = new IntWritable();
            
            int temp = Integer.parseInt(st.nextToken());
            user.set(temp); // set user value
            
            
            //get follower user value from text
        	while (st.hasMoreTokens()) {
        		
        		//convert followers value to int and set it into follow intwritable
            	temp = Integer.parseInt(st.nextToken());
                follow.set(temp);
                
                // emit user and follow key value pair 
                context.write(follow, user);
                
                // emit negatibe user and follow key value pair
                context.write(new IntWritable(-user.get()), new IntWritable(-follow.get()));
            }
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
			
			IntWritable user = key;
			
			StringBuffer stringBuffer = new StringBuffer("");
			
			// generate iterator from iterable
			Iterator<IntWritable> intValues = values.iterator(); 
			
			// take all elements one by one from iterator and store it to string buffer
			while (intValues.hasNext()) {
            	
                int whofollowsuser = intValues.next().get();
                stringBuffer.append(whofollowsuser+ " ");
                
            }
			
			// emit user and result
			Text result = new Text(stringBuffer.toString());
            context.write(user, result);
            
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
