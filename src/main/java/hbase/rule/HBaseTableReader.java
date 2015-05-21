package hbase.rule;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class HBaseTableReader {
	public static void main(String[] args) throws IOException {

		Configuration config = HBaseConfiguration.create();

		HTable table = new HTable(config, "247_session_funnel");
		Scan s = new Scan();
		//s.setStartRow("1425756625663965-1358814214323".getBytes());
		//s.setStopRow(("1425756625663965-1358814214323" + 1).getBytes() );
		Long startTime = new Long("1358814310425");
		Long endTime =  new Long ("1358814561287");
		s.setTimeRange(startTime, endTime);
		ResultScanner scanner = table.getScanner(s);
		try {
			// Scanners return Result instances.
			// Now, for the actual iteration. One way is to use a while loop
			// like so:
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				// print out the row we found and the columns we were looking
				// for
				String key = new String(rr.getRow());
				System.out.print(key +  " # ");
				for (KeyValue kv : rr.list()) {
					int value = Bytes.toInt(kv.getValue());
					String qualifier = Bytes.toString(kv.getQualifier());
					System.out.print(qualifier + " =" +  value + "|");
				}
				System.out.println();
			}

		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}
	}
}
