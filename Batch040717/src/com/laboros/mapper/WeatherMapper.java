package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WeatherMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	
	String fileName=null;

	protected void setup(
			Context context)
			throws java.io.IOException, InterruptedException {
		
		FileSplit filSplit=(FileSplit)context.getInputSplit();
		fileName=filSplit.getPath().getName();
	};

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {

		// key -- 0
		// value -- 27516 20170101 2.424 -156.61 71.32 1.1 -7.5 -3.2 -0.9 9.1
		// 0.00 C 0.1 -7.6 -1.7 96.9 85.3 90.7 -99.000 -99.000 -99.000 -99.000
		// -99.000 -9999.0 -9999.0 -9999.0 -9999.0 -9999.0
		
//		final long lineOffset=key.get();
		
		final String inputLine = value.toString();
		
		if(StringUtils.isNotEmpty(inputLine))
		{
			final String date =StringUtils.substring(inputLine, 6, 14);//20170101
			if(StringUtils.isNotEmpty(date))
			{
			final int year = Integer.parseInt(StringUtils.substring(date, 0, 4));
			
			final String max_temp = StringUtils.substring(inputLine, 38,45);
			context.write(new IntWritable(year), new Text(date+"\t"+max_temp+"\t"+fileName));//(201701014.56)
			}
		}
		
		

	};
	protected void cleanup(
			Context context)
			throws java.io.IOException, InterruptedException {
	};
}
