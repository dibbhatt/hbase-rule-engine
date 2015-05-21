package hbase.rule.classifier;

import static hbase.rule.CreateTables.D_COLUMN_FAMILY_AS_BYTES;
import hbase.rule.engine.RuleEngine;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class ResultClassifier {

    private String ruleFileName;
    private RuleEngine _ruleEngine;
  
    public ResultClassifier(String ruleFilename) {
        this.ruleFileName = ruleFilename;
    }
    
    public void init() {
        _ruleEngine = new RuleEngine(ruleFileName);   
    }

    public void classify(Put put) {
        _ruleEngine.executeRules(put);    
    }
    
    public static void main(String[] args) throws IOException {
        
        ResultClassifier classifier = new ResultClassifier("D:\\work\\workspace\\hbase-rule-engine\\target\\classes\\putRule.drl");
        classifier.init();
        
        LineIterator litr = FileUtils.lineIterator(new File("D:\\work\\workspace\\hbase-rule-engine\\src\\main\\resources\\result.csv"));
        
/*		String[] result = {"1234","15","200","14"};
		Put put = new Put(result[0].getBytes());	
		put.add(COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("sc"), Bytes.toBytes(Float.parseFloat(result[1])));
		put.add(COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("td"), Bytes.toBytes(Integer.parseInt(result[2])));
		put.add(COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("iw"), Bytes.toBytes(Integer.parseInt(result[3])));
		classifier.classify(put);
		
		String decision = Bytes.toString((put.get(Bytes.toBytes("r"), Bytes.toBytes("fr")).get(0)).getValue());
		System.out.println("Score:" + result[1] + " ItemWorked:" + result[3] + " Decision:" + decision);
		
		float score = Bytes.toFloat((put.get(Bytes.toBytes("r"), Bytes.toBytes("sc")).get(0)).getValue());
		
		System.out.println(score);*/


        
        while(litr.hasNext()){
        	
        	try{
        		
        		String[] result = litr.nextLine().split(",");
        		        		
        		if(result[1].equals(""))
        			result[1] = "0";
        		
        		
        		Put put = new Put(result[0].split("/")[3].getBytes());	
        		put.add(D_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("sc"), Bytes.toBytes(Float.parseFloat(result[1])));
        		put.add(D_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("td"), Bytes.toBytes(Integer.parseInt(result[2])));
        		put.add(D_COLUMN_FAMILY_AS_BYTES, Bytes.toBytes("iw"), Bytes.toBytes(Integer.parseInt(result[3])));
        		
        		System.out.println("Score:" + result[1] + " ItemWorked:" + result[3]);

        		classifier.classify(put);
        		
        		String decision = Bytes.toString((put.get(Bytes.toBytes("d"), Bytes.toBytes("fr")).get(0)).getValue());
        		
        		System.out.println("Decision:" + decision);
        		
        		System.out.println("----------");

        		
        	}catch(Exception ex){
        		ex.printStackTrace();
        	}        	
        	
        }       
    }
}