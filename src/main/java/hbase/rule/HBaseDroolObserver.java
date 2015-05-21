package hbase.rule;

import static hbase.rule.CreateTables.D_TABLE_NAME;
import static hbase.rule.CreateTables.R_COLUMN_FAMILY_AS_BYTES;
import static hbase.rule.CreateTables.R_TABLE_NAME;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import hbase.rule.engine.RuleEngine;

public class HBaseDroolObserver extends BaseRegionObserver {
	
	private RuleEngine _ruleEngine = null;

	@Override	
	public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e){
		
		try{
			HTablePool pool = new HTablePool(e.getEnvironment().getConfiguration(),Integer.MAX_VALUE);
			HTableInterface hTable = pool.getTable(R_TABLE_NAME);
			Get get = new Get(Bytes.toBytes("rule-id"));
			get.addColumn(R_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("r"));
			org.apache.hadoop.hbase.client.Result result = hTable.get(get);
			byte[] rule = result.getValue(R_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("r"));
			_ruleEngine = new RuleEngine(rule);
			pool.close();
			hTable.close();
			
		}catch(Exception ex){
			
			ex.printStackTrace();
		}	
	}
	

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {


	}

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
			final Put put, final WALEdit edit, final boolean writeToWAL)
			throws IOException {


		byte[] table = e.getEnvironment().getRegion().getRegionInfo()
				.getTableName();
		if (!Bytes.equals(table, Bytes.toBytes(D_TABLE_NAME)))
			return;
		try {
			if(_ruleEngine != null)
				
				_ruleEngine.executeRules(put);
			
		} catch (Exception ex) {

				ex.printStackTrace();
		}
	}
}
