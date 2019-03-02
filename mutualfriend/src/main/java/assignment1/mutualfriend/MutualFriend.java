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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriend {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] friendList = new String[2];
			int i = 0;
			for(Text value : values) {
				friendList[i++] = value.toString();
			}
			String[] list1 = friendList[0].split(",");
			String[] list2= friendList[1].split(","); 
			LinkedHashSet<String> set1 = new LinkedHashSet<String>();
			set1.addAll(Arrays.asList(list1));

			LinkedHashSet<String> set2 = new LinkedHashSet<String>();
			set2.addAll(Arrays.asList(list2));

			//Find intersection of set 1 and set 2
			set1.retainAll(set2);
			String mutualFriends = set1.toString().replaceAll("\\[|\\]","");
			if(mutualFriends.length() != 0) {
				result.set(new Text(mutualFriends));
			}
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriend <soc-LiveJournal1Adj path> <final output>");
			System.exit(2);
		}
		Job job = Job.getInstance();
		job.setJarByClass(MutualFriend.class);
		job.setJobName("MutualFriend");
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
