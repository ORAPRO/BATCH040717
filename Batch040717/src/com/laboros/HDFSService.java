package com.laboros;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;


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
		printConf(conf);
		
	}
	
	@Override
	public int run(String[] args) throws Exception 
	{
		//logic to connect hdfs and write data to hdfs
		return 0;
	}

	
	private static void printConf(Configuration conf) 
	{
		for (Map.Entry<String, String> entry : conf) {
			
			System.out.println(entry.getKey()+"==="+entry.getValue());
		}
	}
}
