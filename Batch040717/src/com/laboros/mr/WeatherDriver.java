package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.WeatherMapper;
import com.laboros.reducer.WeatherReducer;

public class WeatherDriver extends Configured implements Tool {
	
	public static void main(String[] args) {
		//step -1 : Validations
		
		if(args.length<2)
		{
			System.out.println("java Usage "+WeatherDriver.class.getName()+" [configuration]" +
					" /path/to/hdfs/directory/file /path/to/hdfs/dest/directory"); 
			return;
		}
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i = ToolRunner.run(conf, new WeatherDriver(), args);
			System.out.println("i---->"+i);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILED");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("FAILED");
		}	 	
  	}

	@Override
	public int run(String[] args) throws Exception {
		
		//Total 10 steps
		
		//Step: 1 Get the configuration
		
		Configuration conf = super.getConf();
		
		//set any information explicitly to configuration set here
		
		//step: 2 : Create a Job Instance
		
		Job weatherJob = Job.getInstance(conf, WeatherDriver.class.getName());
		
		//step: 3 : Setting the classpath on datanode for loading mapper for given driver object
		weatherJob.setJarByClass(WeatherDriver.class);
		
		//step : 4 : Setting the input
		
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(weatherJob, hdfsInputPath);
		weatherJob.setInputFormatClass(TextInputFormat.class);
		
		//step: 5 : Setting the output
		final String hdfsOutput = args[1];
		final Path hdfsOuptutPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(weatherJob, hdfsOuptutPath);
		weatherJob.setOutputFormatClass(TextOutputFormat.class);
		
		//step: 6: Setting the mapper

		weatherJob.setMapperClass(WeatherMapper.class);
		//step: 7 : Setting the reducer
		
		weatherJob.setReducerClass(WeatherReducer.class);
		//step: 8 : Setting the mapper output key and value

//		weatherJob.setMapOutputKeyClass(IntWritable.class);
//		weatherJob.setMapOutputValueClass(Text.class);
		//step: 9 : Setting the reducer output key and value
		//TODO set the output key and value
		weatherJob.setOutputKeyClass(IntWritable.class);
		weatherJob.setOutputValueClass(Text.class);
		
		
		//step: 10 : Trigger method
		weatherJob.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}



}
 