package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ur {

  public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

    // Mapper variables
    private IntWritable user1 = new IntWritable();
    private IntWritable user2 = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	// Create a tokenizer to parse each line going through in the format ( 1, 2 )
        StringTokenizer it = new StringTokenizer(value.toString(), "\t"); 

	while(it.hasMoreTokens()) {

		// Set user1 to be the first friend and set user2 to be the second friend
		user1.set(Integer.parseInt(it.nextToken()));
		user2.set(Integer.parseInt(it.nextToken()));

		// Write the key to be the first user and then the values will be all the 
		context.write(user1, user2);
	}

    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {

    // Reducer variables
    private IntWritable recommendation = new IntWritable();

    private String [] linevalues;
    private int user;
    private int likedItem;
    private int friend;
    private boolean itemNotFound = false;

    private int [] userItems = new int[4000];
    private int [] friendItems = new int[4000];
    private int userCount = 0;
    private int friendCount = 0;

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	// Initialize the user and the friend's arrays to hold 0
	for(int i = 0; i < 4000; i++) {
		userItems[i] = 0;
		friendItems[i] = 0;
	}

	// Open ut.txt
	File file = new File("/usr/local/hadoop/ut.txt");
	Scanner inputFile = new Scanner(file);

	// Go through each friend of user1 and provide recommendations from all the friends
	for(IntWritable val: values) {

		// Reset counts to zero for each friend
		userCount = 0;
		friendCount = 0;

		friend = val.get();

		// Go through ut.txt and look for user1 and the current friend's items
		while(inputFile.hasNext()) {
			String familyName = inputFile.nextLine();

			linevalues = familyName.split("\t");
			user = Integer.parseInt(linevalues[0]);
			likedItem = Integer.parseInt(linevalues[1]);

			// If the current user in ut.txt is equal to user1, then save the items in the user1 array
			if(key.get() == user) {
				userItems[userCount] = likedItem;
				userCount++;
				//store the items that the user liked
			}

			// If the current user in ut.txt is equal to the current friend, then save the items in the friend array
			if(friend == user) {
				friendItems[friendCount] = likedItem;
				friendCount++;
				// store the items of the current friend for recommending
			}

		}

		// After parsing through ut.txt, now we need to go through all the items and check to see if an item from the friend's list is not 			in user1's item list
		for(int j = 0; j < friendCount; j++) {
			for(int k = 0; k < userCount; k++) {
				if(friendItems[j] == userItems[k]) {
					itemNotFound = false;
				}
			}
			if(itemNotFound) {
			context.write(key, new IntWritable(friendItems[j]));
			}
			itemNotFound = true;
		}
	}

	// Close ut.txt
	inputFile.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "user user");
    job.setJarByClass(ur.class);
    job.setMapperClass(MyMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

