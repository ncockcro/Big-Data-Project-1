package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ut {

  public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

    // Inialize variables
    private IntWritable user = new IntWritable();
    private IntWritable item = new IntWritable();
    private FloatWritable rating = new FloatWritable();


    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	// Create a tokenizer to parse each line going through in the format ( 1 30 2.5 )
       StringTokenizer it = new StringTokenizer(value.toString(), ",");

	// Set the user to be the first token, items as the second, and rating as the third
	while(it.hasMoreTokens()) {
		user.set(Integer.parseInt(it.nextToken()));
		item.set(Integer.parseInt(it.nextToken()));
		rating.set(Float.parseFloat(it.nextToken()));

		// Any rating greater than or equal to 3 will be sent to reducer
		if(rating.get() >= 3.0) {
			context.write(user, item);
		}
	}

    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {

    // Reducer variables
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	for(IntWritable val : values) {

		context.write(key, val);
	}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "user item");
    job.setJarByClass(ut.class);
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


