package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

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

public class uu {

  public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

    // Mapper variables
    private IntWritable user = new IntWritable();
    private IntWritable item = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	// Create a tokenizer to parse each line going through in the format ( 1,30 )
        StringTokenizer it = new StringTokenizer(value.toString(), "\t");

	while(it.hasMoreTokens()) {

		// Set the user to be the first token and item to be the second
		user.set(Integer.parseInt(it.nextToken()));
		item.set(Integer.parseInt(it.nextToken()));

		// Passing the item as the key and the user as the value
		context.write(item,user);
	}

    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {

    // Reducer variables
    private int count;
    private IntWritable friend1 = new IntWritable();
    private IntWritable friend2 = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	// For each of the friends of an item...
	count = 0;
	for(IntWritable val : values) {
		
		// count is 0, then we will save the first friend and write that person with all of their friends to the output uu.txt
		if(count == 0) {
			friend1.set(val.get());
			count++;
		}
		else {
			friend2.set(val.get());
			context.write(friend1, friend2);
		}
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "user user");
    job.setJarByClass(uu.class);
    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

