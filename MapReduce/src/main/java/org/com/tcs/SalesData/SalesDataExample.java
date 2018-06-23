package org.com.tcs.SalesData;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalesDataExample extends Configured implements Tool{

	private static int status;

	public static void main(String[] args) {
		try {
			 status=ToolRunner.run(new Configuration(), new SalesDataExample(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(status);
	}
	
	public static class myMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		private LongWritable one = new LongWritable(1);
		
		public void map(LongWritable key,Text value,Context context) {
			
			if(key.get()==0)
				return;
			
			String[] tokens = value.toString().split(",");
			try {
				context.write(new Text(tokens[7]), one);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		
		public void reduce(Text key,Iterable<LongWritable> values,Context context) {
			long sum=0;
			for(LongWritable value:values) {
				sum=sum+value.get();
			}
			try {
				context.write(key, new LongWritable(sum));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public int run(String[] arg0) throws Exception {
		// Defining Job
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf,"ProductCount");
		job.setJarByClass(SalesDataExample.class);
		
		//defining input and output arguments
		FileInputFormat.setInputPaths(job,new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		//defining input and output Formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//defining input and output key,values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//setting mapper and reducer
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		
		return job.waitForCompletion(true)?0:1;
	}
}
