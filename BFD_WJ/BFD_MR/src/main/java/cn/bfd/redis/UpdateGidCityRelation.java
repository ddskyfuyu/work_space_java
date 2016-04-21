package cn.bfd.redis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * Created by yu.fu on 2015/4/18.
 */


public class UpdateGidCityRelation {
	
    private Configuration conf;
    private Map<String, Integer> gidFilterMap = new HashMap<String ,Integer>(); 

    public UpdateGidCityRelation(String fin){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
        fillMap(gidFilterMap, fin);
    }
    
    private void fillMap(Map<String, Integer> map, String fin){
        String line = null ;
        try{
        	
            BufferedReader br = new BufferedReader(new FileReader(new File(fin)));
            while ((line = br.readLine()) != null) {
                String[] itemSet = line.trim().split("\t");
                if(itemSet.length != 2){
                	continue;
                }
                else{
                	map.put(itemSet[0], 1);
                }
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fillMap1(Map<String, String> map, String fin){
        String line = null ;
        try{
        	
            BufferedReader br = new BufferedReader(new FileReader(new File(fin)));
            while ((line = br.readLine()) != null) {
                String[] itemSet = line.trim().split("\t");
                if(itemSet.length != 2){
                	continue;
                }
                else{
                	map.put(itemSet[0], itemSet[1]);
                }
            }
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    

    
    public void scanUpdateHbase(String table, String family, String column){
    	ByteArrayOutputStream output=null;
        try {
            HTable hTable = new HTable(conf, Bytes.toBytes(table));
            Scan scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(100);
            ResultScanner reScanner = hTable.getScanner(scanner);
            for(Result result : reScanner){
                String rowkey = new String(result.getRow());
                if(!gidFilterMap.containsKey(rowkey)){
                	continue;
                }
                Put put = new Put(Bytes.toBytes(rowkey));
                put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(gidFilterMap.get(rowkey)));
                hTable.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
    	if(args.length != 4){
			System.out.println("Usage: <fin> <table> <family> <column>");
			System.exit(-1);
    	}
        UpdateGidCityRelation inst = new UpdateGidCityRelation(args[0]);
        inst.scanUpdateHbase(args[1], args[2], args[3]);    
    }

}
