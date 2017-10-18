package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text,
			Text, IntWritable>.Context arg0)
			throws java.io.IOException, InterruptedException {
	};

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value -- DEER RIVER RIVER
//		final long inputKey = key.get();  -- No Usage 
		
		final String inputLine = value.toString();
		
		if(StringUtils.isNotEmpty(inputLine))
		{
			System.out.println("Input Line : "+inputLine);
			
			final String tokens[]=StringUtils.splitPreserveAllTokens(inputLine, " ");
			
			final IntWritable ONE = new IntWritable(1);
			final Text outputKey = new Text();
			for (String word : tokens) {
				outputKey.set(word);
				context.write(outputKey,ONE);
			}
			
		}
		
		
		
		
	};

	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, IntWritable>.Context arg0)
			throws java.io.IOException, InterruptedException {
	};
}
