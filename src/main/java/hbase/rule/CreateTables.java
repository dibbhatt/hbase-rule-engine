package hbase.rule;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTables {

	public static String D_TABLE_NAME = "result";
	public static String D_COLUMN_FAMILY = "d";
	public static byte[] D_COLUMN_FAMILY_AS_BYTES = Bytes.toBytes(D_COLUMN_FAMILY);
	
	public static String R_TABLE_NAME = "rule";
	public static String R_COLUMN_FAMILY = "r";
	public static byte[] R_COLUMN_FAMILY_AS_BYTES = Bytes.toBytes(R_COLUMN_FAMILY);


	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();

		createTableAndColumn(conf, D_TABLE_NAME,
				D_COLUMN_FAMILY_AS_BYTES);
		
		createTableAndColumn(conf, R_TABLE_NAME,
				R_COLUMN_FAMILY_AS_BYTES);
	}

	public static void createTableAndColumn(Configuration conf, String table,
			byte[] columnFamily) throws IOException {
		HBaseAdmin hbase = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(table);
		HColumnDescriptor meta = new HColumnDescriptor(columnFamily);
		desc.addFamily(meta);
		if (hbase.tableExists(table)) {
			if (hbase.isTableEnabled(table)) {
				hbase.disableTable(table);
			}
			hbase.deleteTable(table);
		}
		hbase.createTable(desc);
	}

}
