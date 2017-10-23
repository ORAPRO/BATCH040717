package com.laboros.mapper;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EncryptMapper extends
		Mapper<LongWritable, Text, Text, NullWritable> {

	
	final int[] encryptCols={0,2,3,6,8};
	private static byte[] key = new String("samplekey1234567").getBytes();
	
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws java.io.IOException, InterruptedException {
		
		//key -- 0
		//value -- 11111,bbb1,12/10/1950,1234567890,bbb1@xxx.com,1111111111,M,Diabetes,78 	
	
		final String inputLine = value.toString();
		final StringBuffer outputBuffer  = new StringBuffer();
		if(StringUtils.isNotEmpty(inputLine))
		{
			int iColIdx = 0;
			
			final String[] columns = StringUtils.splitPreserveAllTokens(inputLine, ",");
			
			for (; iColIdx < columns.length; iColIdx++) {
				if(checkColumnIdxToEncrypt(iColIdx))
				{
					outputBuffer.append(encrypt(columns[iColIdx]));
				}else{
					outputBuffer.append(columns[iColIdx]);
				}
				outputBuffer.append("\t");
			}
			
		context.write(new Text(outputBuffer.toString()), NullWritable.get());	
			
		}
	
	}
	private String encrypt(String iColumn) {
		
		try
		{
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
			SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			String encryptedString = Base64.encodeBase64String(cipher.doFinal(iColumn.getBytes()));
			return encryptedString.trim();
		}
		catch (Exception e)
		{
			System.out.println("Error while encrypting"+ e);
		}
		return null;
	}
	private boolean checkColumnIdxToEncrypt(int iColIdx) {
		for (int i = 0; i < encryptCols.length; i++) {
			if(iColIdx==encryptCols[i])
			{
				return Boolean.TRUE;
			}
		}
		return Boolean.FALSE;
	};
}
