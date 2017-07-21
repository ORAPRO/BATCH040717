package com.laboros;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//hadoop fs -put -Ddfs.replication=5 WordCount.txt /user/edureka
public class HDFSService extends Configured implements Tool 
{

	public static void main(String[] args) {

		//Validations
		if(args.length<2)
		{
			System.out.println("Java Usage: HDFSService /path/to/edge/node/local/file /path/to/hdfs/destination/directory");
			return;
		}
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		
		try {
			int returnValue=ToolRunner.run(conf, new HDFSService(), args);
			if(returnValue==0)
			{
				System.out.println("SUCCESS");
			}else{
				System.out.println("FAILURE");
			}
		} catch (Exception e) {
			System.out.println("FAILURE");
			e.printStackTrace();
		}
		
//		printConf(conf);
		
	}
	
	@Override
	public int run(String[] args) throws Exception 
	{
		//logic to connect hdfs and write data to hdfs
		return 0;
	}

	
//	private static void printConf(Configuration conf) 
//	{
//		for (Map.Entry<String, String> entry : conf) {
//			
//			System.out.println(entry.getKey()+"==="+entry.getValue());
//		}
//	}
}
