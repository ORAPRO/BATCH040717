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

import com.laboros.mapper.WordCountMapper;
import com.laboros.partitioner.MyPartitioner;
import com.laboros.reducer.WordCountReducer;

public class WordCountDriver extends Configured implements Tool {
	
	public static void main(String[] args) {
		//step -1 : Validations
		
		if(args.length<2)
		{
			System.out.println("java Usage "+WordCountDriver.class.getName()+" [configuration]" +
					" /path/to/hdfs/directory/file /path/to/hdfs/dest/directory"); 
			return;
		}
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i = ToolRunner.run(conf, new WordCountDriver(), args);
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
		
		Job wordCountJob = Job.getInstance(conf, WordCountDriver.class.getName());
		
		//step: 3 : Setting the classpath on datanode for loading mapper for given driver object
		wordCountJob.setJarByClass(WordCountDriver.class);
		
		//step : 4 : Setting the input
		
		final String hdfsInput = args[0];
		final Path hdfsInputPath = new Path(hdfsInput);
		TextInputFormat.addInputPath(wordCountJob, hdfsInputPath);
		wordCountJob.setInputFormatClass(TextInputFormat.class);
		
		//step: 5 : Setting the output
		final String hdfsOutput = args[1];
		final Path hdfsOuptutPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(wordCountJob, hdfsOuptutPath);
		wordCountJob.setOutputFormatClass(TextOutputFormat.class);
		
		//step: 6: Setting the mapper

		wordCountJob.setMapperClass(WordCountMapper.class);
		//step: 7 : Setting the reducer
		
		wordCountJob.setReducerClass(WordCountReducer.class);
		//step: 8 : Setting the mapper output key and value
		wordCountJob.setCombinerClass(WordCountReducer.class);
		wordCountJob.setNumReduceTasks(3);
		wordCountJob.setPartitionerClass(MyPartitioner.class);

//		wordCountJob.setMapOutputKeyClass(Text.class);
		wordCountJob.setMapOutputValueClass(IntWritable.class);
		//step: 9 : Setting the reducer output key and value
		//TODO set the output key and value
		wordCountJob.setOutputKeyClass(Text.class);
		wordCountJob.setOutputValueClass(IntWritable.class);
		
		
		//step: 10 : Trigger method
		wordCountJob.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}



}
 