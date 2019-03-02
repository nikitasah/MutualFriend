package assignment1.mutualfriend;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriendInMemoryJoin extends Configured implements Tool{

	static HashMap<String, String> userDataHashMap;
	static String userA = "";
	static String userB = "";

	// Map 1 : Finds the mutual friends of user pair from input file
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text userPair = new Text();
		private Text pairMutualfriends = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			userA = config.get("userA");
			userB = config.get("userB");
			String[] textSplitArray = value.toString().split("\\t");

			if ((textSplitArray.length == 2) && (textSplitArray[0].equals(userA) || textSplitArray[0].equals(userB))) {
				pairMutualfriends.set(textSplitArray[1]);
				String[] userFriendArray = textSplitArray[1].split(",");
				for (int i = 0; i < userFriendArray.length; i++) {
					String userKey;
					userKey = Integer.parseInt(textSplitArray[0]) < Integer.parseInt(userFriendArray[i]) ?
							textSplitArray[0] + "," + userFriendArray[i] : userFriendArray[i] + "," + textSplitArray[0];
							userPair.set(userKey);
							context.write(userPair, pairMutualfriends);
				}
			} 
		}
	}

	// Reduce1
	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String> hashSet = new HashSet<String>();
			int firstIteration = 0;
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				String[] friendsArray = value.toString().split(",");
				for (int j = 0; j < friendsArray.length; j++) {
					if (firstIteration == 0) {
						hashSet.add(friendsArray[j]);
					} else {
						if (hashSet.contains(friendsArray[j])) {
							if(sb.length() == 0) {
								sb.append(friendsArray[j]);
							}
							else {
								sb.append("," + friendsArray[j]);
							}
							hashSet.remove(friendsArray[j]);
						}
					}
				}
				firstIteration++;
			}
			if (sb.length() != 0) {
				result.set(sb.toString());
				context.write(key, result);
			}
		}
	}


	//Mapper2 : Implementation of In Memory Join
	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {

		private Text nameCity = new Text();
		private Text userPair = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			userDataHashMap = new HashMap<String, String>();
			Configuration config = context.getConfiguration();
			String userdataPath = config.get("userdata");
			FileSystem fp = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fp.open(new Path(userdataPath))));
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] splitArray = line.split(",");
				if (splitArray.length == 10) {
					String mapValue = splitArray[1] + ":" + splitArray[4];
					userDataHashMap.put(splitArray[0].trim(), mapValue);
				}
			}

			String[] pairMutualFriends = value.toString().split("\\t");
			if (pairMutualFriends.length == 2) {
				String[] mutualFriendsArray = pairMutualFriends[1].split(",");
				StringBuilder result = new StringBuilder("[");

				for (int i = 0; i < mutualFriendsArray.length; i++) {
					if (userDataHashMap.containsKey(mutualFriendsArray[i])) {
						result.append((userDataHashMap.get(mutualFriendsArray[i])) + ",");
					}
				}
				if(result.charAt(result.length() - 1) == ',') {
					result.deleteCharAt(result.length() - 1);
				}
				result.append("]");
				userPair.set(pairMutualFriends[0]);
				nameCity.set(result.toString());
				context.write(userPair, nameCity);
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		int flag = ToolRunner.run(new Configuration(), new MutualFriendInMemoryJoin(), args);
		System.exit(flag);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 5) {
			System.err.println("<userA> <userB> <soc-LiveJournal1Adj path> <userdata path> <final output>");
			System.exit(2);
		}
		conf.set("userA", args[0]);
		conf.set("userB", args[1]);

		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "MutualFriendsMemoryJoin");
		job1.setJarByClass(MutualFriendInMemoryJoin.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[2]));
		FileOutputFormat.setOutputPath(job1, new Path(args[4] + "/temp"));
		int flag = job1.waitForCompletion(true) ? 0 : 1;

		Configuration conf2 = getConf();
		conf2.set("userdata", args[3]);

		@SuppressWarnings("deprecation")
		Job job2 = new Job(conf2, "InMemoryJoin");
		job2.setJarByClass(MutualFriendInMemoryJoin.class);
		job2.setMapperClass(Mapper2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[4] + "/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[4] + "/final"));
		flag = job2.waitForCompletion(true) ? 0 : 1;
		System.exit(flag);
		return flag;
	}

}
