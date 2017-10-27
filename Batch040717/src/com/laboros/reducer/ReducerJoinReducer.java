package com.laboros.reducer;

import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerJoinReducer extends
		Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(
			Text key,
			java.lang.Iterable<Text> values,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key --- 4000001
		//values -- {CUSTS	Kristina, TXNS	180.35,TXNS	051.18}
		String name=null;
		int count=0;
		double sum=0;
		
		
		Iterator<Text> inputValues=values.iterator();
		while(inputValues.hasNext())
		{
			Text inputValue = inputValues.next();
			final String strInput= inputValue.toString();
			final String[] tokens = StringUtils.splitPreserveAllTokens(strInput, "\t");
			if(StringUtils.equals("TXNS", tokens[0]))
			{
				count = count+1;
				sum = sum+Double.parseDouble(tokens[1]);
			}else if(StringUtils.equals("CUSTS", tokens[0]))
			{
				name = tokens[1];
			}
		}
		if(StringUtils.isNotEmpty(name))
		{
		context.write(new Text(name), new Text(count+"\t"+sum));
		}
	};
}
