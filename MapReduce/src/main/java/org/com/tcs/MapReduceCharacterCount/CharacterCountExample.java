package org.com.tcs.MapReduceCharacterCount;

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

public class CharacterCountExample extends Configured implements Tool {

	public static void main(String[] args) {
		int value = 0;
		try {
			value = ToolRunner.run(new Configuration(), new CharacterCountExample(), args);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(value);
	}
	
	public static class myMapper extends Mapper<Object,Text,Text,LongWritable>{
		private	LongWritable one = new LongWritable(1);
		
		public void map(Object key,Text value,Context context) {
			String[] tokens = value.toString().split("\t",-3);
			for(String token:tokens) {
				for(int i=0;i<token.length();i++) {
					try {
						context.write(new Text(token.substring(i, i+1)), one);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	public static class myReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		public void reduce(Text key,Iterable<LongWritable> values,Context context) {
			long sum=0;
			for(LongWritable value:values) {
				sum= sum+value.get();
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
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf,"character count");
		job.setJarByClass(CharacterCountExample.class);
		
		//set input and output parameters
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		
		//setting input output formats
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//setting output keys and values
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//setting mapper and reducer
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		return job.waitForCompletion(true) ? 0 : 1;

	}

}
