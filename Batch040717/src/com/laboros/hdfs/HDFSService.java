package com.laboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

  
// hadoop fs -put WordCount.txt /user/edureka

// java com.laboros.hdfs.HDFSService WordCount.txt /user/edureka abc
public class HDFSService extends Configured implements Tool {

	public static void main(String[] args) {
		//step -1 : Validations
		
		if(args.length<2)
		{
			System.out.println("java Usage HDFSService [configuration]" +
					" /path/to/edgenode/local/file /path/to/hdfs/dest/directory"); 
			return;
		}
		
		Configuration conf = new Configuration(Boolean.TRUE);
		conf.set("fs.defaultFS", "hdfs://localhost:8020");
		
		try {
			int i = ToolRunner.run(conf, new HDFSService(), args);
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
		//Writing data =  creating metadata + Add data
		//Creating metadata = creating empty file
		
		final String hdfsDestDir = args[1];
		final String edgeNodeLocalFile = args[0]; //WordCount.txt, /home/edureka/SAIWS/BATCH040717/Datasets/WordCount.txt
		
		final String edgeNodeParsedLocalFile = getFileName(edgeNodeLocalFile);

		final Path hdfsDestDirWithFileName = new Path(hdfsDestDir, edgeNodeParsedLocalFile); //should not contain / for edgeNodeParsedLocalFile
		
		//Get the configuration
		
		Configuration conf = super.getConf();
		
		FileSystem hdfsFS = FileSystem.get(conf);
		
		FSDataOutputStream fsdos=hdfsFS.create(hdfsDestDirWithFileName, Boolean.FALSE);
		
		
		//Adding data =
		//step-1 : split data into blocks
		
		InputStream is = new FileInputStream(edgeNodeLocalFile);
		
		
		
		//step-2 : Identify the datanode on which i need to write the data block
		//step-3: Writing data to datanode
		//step-4 : Write replication
		//step-5: Sync with namenode
		//step-6 : Handle Failures
		
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		
		return 0;
	}


	private String getFileName(String edgeNodeLocalFile) {
		return edgeNodeLocalFile;
	}

	
	
	
	
	
	
	
	
	
	
	

}
