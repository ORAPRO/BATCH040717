package com.laboros.reducer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WeatherReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	protected void reduce(IntWritable key, java.lang.Iterable<Text> values,
			Context context) throws java.io.IOException, InterruptedException {
		// key -- 2017
//		final int inputKey = key.get();

		String inputDate = null;
		double inputTemp = Double.MIN_VALUE;

		double temp = Double.MIN_VALUE;
		String fileName = null;
		for (Text inputValues : values) {
			final String tokens[] = StringUtils.splitPreserveAllTokens(
					inputValues.toString(), "\t");
			inputTemp = Double.parseDouble(tokens[1]);
			if(temp < inputTemp)
			{
				temp=inputTemp;
				inputDate = tokens[0]; //20170101
				fileName=tokens[2];
					
			}
			
		}
		context.write(key, new Text(temp+"\t"+inputDate+"\t"+fileName));

	};
}
