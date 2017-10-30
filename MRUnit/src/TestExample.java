import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.*;
import org.junit.Test;


public class TestExample extends TestCase {
		
		public static class MapTest extends Mapper<LongWritable, Text, Text, IntWritable>{
			Text day=new Text();
			
			public void map(LongWritable key,Text value,Context ct)throws IOException,InterruptedException
			{
				//key --0
				//value -- 1,sunday,abhay,holiday
				String[] word=value.toString().split(",");
				//word[0]--- 1
				//word[1]-- sunday
				//word[2] -- abhay
				//word[3] -- holiday
				int val=Integer.parseInt(word[0]);//---> 1
				day.set(word[1]); //-------> sunday
				ct.write(day,new IntWritable(val));//-->sunday,1
				
			}
		}
		 MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
		 
  public void setUp() {
	  System.setProperty("hadoop.home.dir", "C://Users//suresh//Downloads//MRUnit-master//MRUnit-master//MRUnit//lib//");
    mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>(new MapTest());
  }

  @Test
  public void testMapper() {
		mapDriver.withInput(new LongWritable(), new Text("1,sunday,abhay,holiday"))
		        .withOutput(new Text("sunday"), new IntWritable(2))
		        .runTest();
  }
}