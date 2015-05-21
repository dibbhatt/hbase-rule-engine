package hbase.rule;

import static hbase.rule.CreateTables.D_COLUMN_FAMILY_AS_BYTES;
import static hbase.rule.CreateTables.D_TABLE_NAME;


import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class DataLoader {

	 
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		int version = 1;

		HTable htable = new HTable(conf, D_TABLE_NAME);
		htable.setAutoFlush(true);
        LineIterator litr = FileUtils.lineIterator(new File(args[0]));

        while(litr.hasNext()){
        	
        	try{
        		String[] result = litr.nextLine().split(",");
        		if(result[1].equals(""))
        			result[1] = "0";
        		
        		Put put = new Put(result[0].split("/")[3].getBytes());	
        		put.add(D_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("sc"), Bytes.toBytes(Float.parseFloat(result[1])));
        		put.add(D_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("td"), Bytes.toBytes(Integer.parseInt(result[2])));
        		put.add(D_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("iw"), Bytes.toBytes(Integer.parseInt(result[3])));
        		htable.put(put);
        	}catch(Exception ex){
        		//ex.printStackTrace();
        	}        	
        	
        }
	}	
}
