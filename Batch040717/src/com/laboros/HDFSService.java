package com.laboros;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class HDFSService extends Configured implements Tool 
{

	//Java Usage: HDFSService /path/to/edge/node/local/file /path/to/hdfs/destination/directory
	public static void main(String[] args) {

		//Validations
		if(args.length < 2){
			System.out.println("Invalid no.of arguments");
			return;
		}
		
		Configuration config = new Configuration();
		printConfig(config);
	}
	
	private static void printConfig(Configuration config) {
		
		for(Map.Entry<String, String> entry : config){
			System.out.println(entry.getKey()+" = "+entry.getValue());
		}
		
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		//logic to connect hdfs and write data to hdfs
		return 0;
	}

}
