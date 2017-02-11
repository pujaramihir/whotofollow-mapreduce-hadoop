import java.awt.Point;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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

			// Key is ignored as.i""t only stores the offset of the line in the
			// text file
			StringTokenizer st = new StringTokenizer(values.toString());

			// user and follow will be the elements in the emitted pairs.
			IntWritable user = new IntWritable();
			IntWritable follow = new IntWritable();

			int temp = Integer.parseInt(st.nextToken());
			user.set(temp); // set user value

			// get follower user value from text
			while (st.hasMoreTokens()) {

				// convert followers value to int and set it into follow
				// intwritable
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
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			IntWritable user = key;

			StringBuffer stringBuffer = new StringBuffer("");

			// generate iterator from iterable
			Iterator<IntWritable> intValues = values.iterator();

			// take all elements one by one from iterator and store it to string
			// buffer
			while (intValues.hasNext()) {

				int whofollowsuser = intValues.next().get();
				stringBuffer.append(whofollowsuser + " ");

			}

			// emit user and result
			Text result = new Text(stringBuffer.toString());
			context.write(user, result);
		}
	}

	/**
	 * Second Mapper Class to find the Similarity
	 * 
	 * @author Mohit Pujara
	 *
	 */
	public static class MapperSecond extends Mapper<Object, Text, IntWritable, IntWritable> {

		public void map(Object key, Text values, Context context) throws IOException, InterruptedException {

			// String Tockenizer used to separate values from text file
			StringTokenizer st1 = new StringTokenizer(values.toString());
			StringTokenizer st2 = new StringTokenizer(values.toString());

			IntWritable follower1 = new IntWritable();
			IntWritable follower2 = new IntWritable();
			
			ArrayList<Point> data = new ArrayList<Point>();

			int temp;

			int first = Integer.parseInt(st1.nextToken());
			st2.nextToken();

			if (first < 0) {

				// Emits Negative values from key value pairs
				while (st1.hasMoreTokens()) {

					follower1.set(Math.abs(first));
					follower2.set(Integer.parseInt(st1.nextToken()));

					context.write(follower1, follower2);
				}
				return;
			}

			// Generate key value pair (yi, yj) and (yj, yi)
			while (st1.hasMoreTokens()) {

				follower1.set(Integer.parseInt(st1.nextToken()));

				while (st2.hasMoreTokens()) {

					temp = Integer.parseInt(st2.nextToken());

					if (follower1.get() != temp) {

						follower2.set(temp);

						Point p = new Point(follower1.get(), follower2.get());
						if (!data.contains(p)) {
							data.add(p);
							context.write(follower1, follower2);
						}
					}
				}
				st2 = new StringTokenizer(values.toString());
				st2.nextToken();
			}
		}
	}

	/**
	 * ReducerSecond class finds recommended users and common friends between them 
	 * 
	 * @author mohit
	 *
	 */
	public static class ReducerSecond extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

		public class Followers {
			public int positive = 0;
			public int negative = 0;
			public int count = 0;
		}

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			IntWritable user = key;
			HashMap<Integer, Followers> data = new HashMap<Integer, Followers>();
			StringBuffer stringBuffer = new StringBuffer("");
			Iterator<IntWritable> i = values.iterator();
			Iterator<Integer> s = data.keySet().iterator();
			ArrayList<Followers> finalData = new ArrayList<Followers>();
			Iterator<Followers> d = finalData.iterator();

			// Stores number of followed users and counts
			while (i.hasNext()) {

				int whofollowsuser = i.next().get();

				if (data.containsKey(Math.abs(whofollowsuser))) {
					Followers followers = data.get(Math.abs(whofollowsuser));
					if (whofollowsuser < 0) {
						followers.negative = whofollowsuser;
					} else {
						followers.positive = whofollowsuser;
						++followers.count;
					}
				} else {
					Followers followers = new Followers();
					if (whofollowsuser < 0) {
						followers.negative = whofollowsuser;
					} else {
						followers.positive = whofollowsuser;
						++followers.count;
					}
					data.put(Math.abs(whofollowsuser), followers);
				}
			}
			
			// Removes the number and its negation if found, also removed remaining negative numbers if any
			while (s.hasNext()) {
				Followers f = data.get(s.next());
				if (!(f.positive == Math.abs(f.negative)) && f.positive > 0) {

					if (f.count > 0) {
						finalData.add(f);
					}
				}
			}
			
			// sorts the recommended users according more common friends
			finalData.sort(new Comparator<Followers>() {
				@Override
				public int compare(Followers o1, Followers o2) {
					if (o2.count > o1.count) {
						return 1;
					} else if (o2.count < o1.count) {
						return -1;
					}
					return 0;
				}
			});
			
			// Append the recommended users and its common user count
			while (d.hasNext()) {
				Followers f = d.next();

				stringBuffer.append(f.positive + "(" + f.count + ") ");
			}
		}
	}

	public static void main(String[] args)
			throws ClassNotFoundException, IllegalStateException, IllegalArgumentException, InterruptedException {

		// create configuration
		Configuration conf = new Configuration();

		// create job for first map reduce task
		Job firstJob;
		try {

			firstJob = Job.getInstance(conf, "who to follow first job");
			firstJob.setJarByClass(whotofollow.class);
			firstJob.setMapperClass(MapperFirst.class);
			firstJob.setReducerClass(ReducerFirst.class);
			firstJob.setOutputKeyClass(IntWritable.class);
			firstJob.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(firstJob, new Path(args[0]));
			FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));

			// Check whether first job is completed or not
			if (firstJob.waitForCompletion(true)) {

				// second job
				Configuration conf2 = new Configuration();
				Job secondJob = Job.getInstance(conf2, "who to follow second job");
				secondJob.setJarByClass(whotofollow.class);
				secondJob.setMapperClass(MapperSecond.class);
				secondJob.setReducerClass(ReducerSecond.class);
				secondJob.setOutputKeyClass(IntWritable.class);
				secondJob.setOutputValueClass(IntWritable.class);

				FileInputFormat.addInputPath(secondJob, new Path(args[1]));
				FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));

				System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
