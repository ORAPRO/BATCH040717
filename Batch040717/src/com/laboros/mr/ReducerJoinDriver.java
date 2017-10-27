package com.laboros.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.laboros.mapper.CustomerMapper;
import com.laboros.mapper.TxnMapper;
import com.laboros.reducer.ReducerJoinReducer;

// yarn jar ADMRDemo.jar com.laboros.mr.ReducerJoinDriver custs txns RJOP
public class ReducerJoinDriver extends Configured implements Tool {
	
	public static void main(String[] args) {
		//step -1 : Validations
		
		if(args.length<3)
		{
			System.out.println("java Usage "+ReducerJoinDriver.class.getName()+" [configuration]" +
					" /path/to/hdfs/directory/customer_file " +
					" /path/to/hdfs/directory/transaction_file " +
					"/path/to/hdfs/dest/directory"); 
			return;
		}
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			int i = ToolRunner.run(conf, new ReducerJoinDriver(), args);
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
		
		Job reducerJoinJob = Job.getInstance(conf, ReducerJoinDriver.class.getName());
		
		//step: 3 : Setting the classpath on datanode for loading mapper for given driver object
		reducerJoinJob.setJarByClass(ReducerJoinDriver.class);
		
		//step : 4 : Setting the input
		
		//step: 4-a : Setting the input for the Customer
		final String customerInput = args[0];
		final Path customerInputPath = new Path(customerInput);
		MultipleInputs.addInputPath(reducerJoinJob, customerInputPath, TextInputFormat.class, CustomerMapper.class);
		
		
		//step:4-b : Setting the input for the transaction
		
		final String txnInput = args[1];
		final Path txnInputPath = new Path(txnInput);
		MultipleInputs.addInputPath(reducerJoinJob, txnInputPath, TextInputFormat.class, TxnMapper.class);
		
		
		//step: 5 : Setting the output
		final String hdfsOutput = args[2];
		final Path hdfsOuptutPath = new Path(hdfsOutput);
		TextOutputFormat.setOutputPath(reducerJoinJob, hdfsOuptutPath);
		reducerJoinJob.setOutputFormatClass(TextOutputFormat.class);
		
		//step: 6 : Setting the reducer
		
		reducerJoinJob.setReducerClass(ReducerJoinReducer.class);
		//step: 8 : Setting the mapper output key and value

		reducerJoinJob.setMapOutputKeyClass(Text.class);
		reducerJoinJob.setMapOutputValueClass(Text.class);
		//step: 9 : Setting the reducer output key and value
		//TODO set the output key and value
//		reducerJoinJob.setOutputKeyClass(Text.class);
//		reducerJoinJob.setOutputValueClass(IntWritable.class);
		
		
		//step: 10 : Trigger method
		reducerJoinJob.waitForCompletion(Boolean.TRUE);
		
		return 0;
	}



}
 