package com.laboros.mapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CustomerMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		//key -- 0 ;
		//value -- 4000001,Kristina,Chung,55,Pilot
		
		final String inputLine = value.toString();
		if(StringUtils.isNotEmpty(inputLine))
		{
			final String[] columns = StringUtils.splitPreserveAllTokens(inputLine, ",");
			if(StringUtils.isNotEmpty(columns[0]) && StringUtils.isNotEmpty(columns[1]))
			{
				System.out.println("Columns[0"+columns[0]);
				
			context.write(new Text(columns[0]), new Text("CUSTS\t"+columns[1]));
			}
		}
		
	};
}
