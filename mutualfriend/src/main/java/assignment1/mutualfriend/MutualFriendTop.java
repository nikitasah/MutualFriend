package assignment1.mutualfriend;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriendTop {

	// Mapper 1
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>{
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] splitArray = value.toString().split("\t");

			if(splitArray.length == 1) {
				return;
			}
			int userId = Integer.parseInt(splitArray[0]);
			List<String> others = Arrays.asList(splitArray[1].split(","));
			for(String friend : others) {
				int friend2 = Integer.parseInt(friend);
				if(friend2 == userId) {
					continue;
				}
				if(userId > friend2) {
					word.set(friend2 + "," + userId);
				}
				else {
					word.set(userId + "," + friend2);
				}
				context.write(word, new Text(splitArray[1]));
			}
		}
	}

	// Reducer 1
	public static class Reduce1 extends Reducer<Text, Text, Text, LongWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] friendList = new String[2];
			int i = 0;
			long countMututalFriend = 0;
			for(Text value : values) {
				friendList[i++] = value.toString();
			}
			String[] list1 = friendList[0].split(",");
			String[] list2= friendList[1].split(","); 

			LinkedHashSet<String> set1 = new LinkedHashSet<String>();
			set1.addAll(Arrays.asList(list1));
			for(String s : list2) {
				if(set1.contains(s)) {
					countMututalFriend++;
				}
			}
			context.write(key, new LongWritable(countMututalFriend));
		}
	}

	// Mapper 2
	public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				String[] line = value.toString().split("\t");
				Text value1 = new Text(line[0]); // Friend pair 
				LongWritable key1 = new LongWritable(Long.parseLong(line[1]));//mutual friend count
				context.write(key1, value1);
			}
		}
	}

	// Reducer 2
	public static class Reduce2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		static private int count = 0;

		public  void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (count >= 10) {
				return;
			}
			for(Text value : values) {
				if (count >= 10) {
					return;
				}
				context.write(value, key);
				count++;
			} 
		}
	}

	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriendTop <soc-LiveJournal1Adj path> <final output>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MutualFriendTop 1");
		job.setJarByClass(MutualFriendTop.class);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		// Set the output path for map-reduce 1 with the new path /Users/nikita/output/temp
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] +"/temp" ));
		boolean complete = job.waitForCompletion(true);

		if (complete) {
			Configuration conf1 = new Configuration();
			@SuppressWarnings("deprecation")
			Job job1 = new Job(conf1, "MutualFriendTop 2");
			job1.setJarByClass(MutualFriendTop.class);
			job1.setMapperClass(Map2.class);
			job1.setReducerClass(Reduce2.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapOutputKeyClass(LongWritable.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setNumReduceTasks(1);	
			job1.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			job1.setOutputKeyClass(Text.class);

			job1.setOutputValueClass(LongWritable.class);

			// Sending output of MapReduce1 as an input to MapReduce2 
			FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+"/temp"));

			// Set the output path for map-reduce 2 with the new path /Users/nikita/output/final
			FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/final"));
			System.exit(job1.waitForCompletion(true) ? 0 : 1);
		}
	}
}
