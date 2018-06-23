package org.com.tcs.MapReduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class EmpMaxSalaryExample extends Configured implements Tool
{
	public static class my_Mapper extends Mapper<LongWritable,Text,Text,Text>{
		
		public void map(LongWritable key,Text value,Context context) {
			String[] tokens = value.toString().split("\t",-3);
			String gender = tokens[3];
			try {
				context.write(new Text(gender), value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static class my_Reducer extends Reducer<Text,Text,Text,IntWritable>{
		
		public void reduce(Text key,Iterable<Text> values,Context context) {
			
			int Max = 0;
			for(Text value : values) {
				String[] tokens = value.toString().split("\t",-3);
				int salary = Integer.parseInt(tokens[4]);
				if(salary>Max)
					Max=salary;
			}
			
			try {
				context.write(key, new IntWritable(Max));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
    public static void main( String[] args )
    {
    	try {
			int res = ToolRunner.run(new Configuration(), new EmpMaxSalaryExample(), args);
			System.out.println(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.exit(0);
    }

    
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(new Configuration(),"MaxSalaryEmployee");
		job.setJarByClass(EmpMaxSalaryExample.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job,new Path(arg0[1]));
		
		job.setMapperClass(my_Mapper.class);
		job.setReducerClass(my_Reducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	      System.exit(job.waitForCompletion(true)? 0 : 1);
	      return 0;
	}

}
