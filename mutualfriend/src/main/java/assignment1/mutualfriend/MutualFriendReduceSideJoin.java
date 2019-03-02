package assignment1.mutualfriend;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MutualFriendReduceSideJoin {


	// Map-Reduce Phase 1
	// Output : Key : user Value : List of friends
	public static class FriendsMapper1 extends Mapper<LongWritable, Text, Text, Text> {
		static final String tag = "friends\t";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArray = value.toString().split("\t");
			if (splitArray.length == 2) {
				String userId = splitArray[0];
				String userFriends = splitArray[1];
				context.write(new Text(userId), new Text(tag + userFriends));
			}
		}
	}

	//  Output : Key : user Value : userdetails + currentAge
	public static class UserMapper1 extends Mapper<Object, Text, Text, Text> {
		static final String tag = "userdetails\t";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArray = value.toString().split(",");
			Date dob = new Date();
			DateFormat inputDateFormat = new SimpleDateFormat("MM/dd/yyyy");
			try {
				dob = inputDateFormat.parse(splitArray[9]);
			} catch (ParseException ex) {
				System.out.println("ParseException : " + ex);
			}
			Date current = new Date();
			String currentAge = Long.toString(current.getTime() - dob.getTime());
			// Key is userid , Value is current Age
			context.write(new Text(splitArray[0]), new Text(tag + currentAge));
		}
	}

	//  Output :  Key : friend Value : age
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String ageFriend = "0";
			String friends = new String();
			// Searching tag to segregate between user and friends
			for (Text value : values) {
				String[] splitArray = value.toString().split("\t");
				String tag = splitArray[0];
				if (tag.equals("friends")) {
					friends = splitArray[1];
				} else {
					ageFriend = splitArray[1];
				}	    
			}		
			// Produce output as key friend and age value
			String[] friendsArray = friends.split(",");
			for (String friend : friendsArray) {
				context.write(new Text(friend), new Text(ageFriend));
			}
		}
	}

	// Map-Reduce Phase 2
	// Output :  Key : userId Value :friends\t + friendAge
	public static class FriendMapper2 extends Mapper<LongWritable, Text, Text, Text> {

		static final String tag = "friends\t";

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArray = value.toString().split("\t");
			if (splitArray.length == 2) {
				String userId = splitArray[0];
				String friendAge = splitArray[1];
				context.write(new Text(userId), new Text(tag + friendAge));
			}
		}

	}

	//  Output : Key : userId Value :useraddress\t + address
	public static class UserMapper2 extends Mapper<Object, Text, Text, Text> {
		static final String tag = "useraddress\t";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArray = value.toString().split(",");
			Text address = new Text(splitArray[3] + "," + splitArray[4] + "," + splitArray[5]);//address(street,city,state)
			context.write(new Text(splitArray[0]), new Text(tag + address));
		}
	}

	//  Output :  Key : averageAge Value : user + \t + address
	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
		private final long msecsInYear = 31536000000L;//1000*60*60*24*365
		//private final long msecsInDay = 86400000L;//1000*60*60*24
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Long sumAge = (long) 0;
			int count = 0;
			double averageAge;
			String address = new String();
			for (Text value : values) {
				String[] splitArray = value.toString().split("\t");
				String tag = splitArray[0];
				if (tag.equals("useraddress")) {
					address = splitArray[1];
					continue;
				}
				else {
					//tag = friends
					Long age = Long.parseLong(splitArray[1]);
					sumAge += age;
					count++;
				}

			}
			if(count > 0) {
				long ageYears = (long)Math.floor(sumAge/(msecsInYear));
				//long ageDays = (long)Math.floor((sumAge % msecsInYear) / (msecsInDay));
				averageAge = (double)ageYears/count;
				Text avgAgeKey = new Text(Double.toString(averageAge));//output key
				Text addressVal = new Text(key + "\t" + address);
				context.write(avgAgeKey, addressVal);
			}
		}
	}

	// Map-Reduce Phase 3
	//  Output :  Key : averageAge Value : userId + \t + address
	public static class Mapper3 extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArray = value.toString().split("\t");
			if (splitArray.length == 3) {
				DoubleWritable avgAge = new DoubleWritable(Double.parseDouble(splitArray[0]));
				String userId = splitArray[1];
				String userAddress = splitArray[2];
				context.write(avgAge, new Text(userId + "\t" + userAddress));
			}
		}
	}
	
	//  Output :  Key : UserId Value : userAddress + \t + Age
	public static class Reduce3 extends Reducer<DoubleWritable, Text, Text, Text> {

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			DecimalFormat decimalFormat = new DecimalFormat("#0.0000");
			for (Text value : values) {
				String[] splitArray = value.toString().split("\t");
				String userAddress = splitArray[1];
				context.write(new Text(splitArray[0]) , new Text(userAddress + "\t" + decimalFormat.format(key.get())));
			}     		
		}
	}

	public static void main(String[] args)throws Exception  {

		if (args.length != 3) {
			System.err.println("<soc-LiveJournal1Adj path> <userdata path> <final output>");
			System.exit(2);
		}
		//First phase of Map Reduce
		Configuration conf1 = new Configuration();
		Job job = Job.getInstance(conf1, "MutualFriendReduceSideJoinPhase1");
		job.setJarByClass(MutualFriendReduceSideJoin.class);
		job.setReducerClass(Reduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, FriendsMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, UserMapper1.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "/temp1"));
		// Wait till job completion
		job.waitForCompletion(true);

		//Second phase of mapreduce jobs
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "MutualFriendReduceSideJoinPhase2");
		job2.setJarByClass(MutualFriendReduceSideJoin.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job2, new Path(args[2] + "/temp1"), TextInputFormat.class, FriendMapper2.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, UserMapper2.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/temp2"));
		job2.waitForCompletion(true);

		//Third phase of mapreduce jobs
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "MutualFriendReduceSideJoinPhase3");
		job3.setJarByClass(MutualFriendReduceSideJoin.class);
		job3.setOutputKeyClass(DoubleWritable.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reduce3.class);
		job3.setNumReduceTasks(1);
		job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);	
		FileInputFormat.addInputPath(job3, new Path(args[2] + "/temp2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/final"));
		job3.waitForCompletion(true);

	}

}
