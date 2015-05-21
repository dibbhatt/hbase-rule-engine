package hbase.rule;

import static hbase.rule.CreateTables.R_COLUMN_FAMILY_AS_BYTES;
import static hbase.rule.CreateTables.R_TABLE_NAME;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class RuleLoader {

	 
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable htable = new HTable(conf, R_TABLE_NAME);
		htable.setAutoFlush(true);
        byte[] rule = FileUtils.readFileToByteArray(new File(args[0]));
        
		Put put = new Put("stratarule".getBytes());	
		put.add(R_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("result"), rule);
		htable.put(put);

	}	
}
