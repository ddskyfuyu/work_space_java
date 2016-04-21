package cn.bfd.redis;


import cn.bfd.protobuf.PortraitOuterClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;



/**
 * Created by yu.fu on 2015/4/18.
 */


public class UpdateUserProfile {
	
	private final static Logger logger = Logger.getLogger(UpdateUserProfile.class);
    //private RedisClient client;
	JedisPool pool  = null;
    private Configuration conf;

    public UpdateUserProfile(String ip, int port, String fin, String password){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxTotal(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);
        config.setTestWhileIdle(true);
        config.setTimeBetweenEvictionRunsMillis(10*1000);
        pool=new JedisPool(config,ip, port, 1000, password);
    }

    
    public boolean setValue2Redis(Map<String, String> map, String key, String value, String bussine_id,Jedis jedis){
    	String m_key = bussine_id + ":" + key;
    	if(map.containsKey(m_key)){
    	    jedis.set(bussine_id + ":" + key, value);
    	    return true;
    	}
    	return false;
    }
    
    public void setValue2Redis(String key, byte [] value, String bussine_id, Jedis jedis){
    	try {
			jedis.set((bussine_id + ":" + key).getBytes("utf-8"), value);
			logger.debug("Successful rowkey: " + key);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			logger.debug("Error rowkey: " + key);
		}
    }
    
    

    
    public void scanResult2Redis(String table, String family, String column, String u_id){
    	ByteArrayOutputStream output=null;
        try {
            HTable hTable = new HTable(conf, Bytes.toBytes(table));
            Scan scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(100);
            ResultScanner reScanner = hTable.getScanner(scanner);
            Jedis jedis = pool.getResource();
            for(Result result : reScanner){
                String rowkey = new String(result.getRow());
                logger.debug("Rowkey: " + rowkey);
                PortraitOuterClass.Portrait up = null;
                try{
                	up = PortraitOuterClass.Portrait.parseFrom(result.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
                }catch(IOException e){
                	logger.error("rowkey: " + rowkey);
                	e.printStackTrace();
                	continue;
                }
                if(!up.hasUuid()){
                    continue;
                }
                String[] keySet = up.getUuid().split(":");
                if(keySet.length != 2){
                    continue;
                }
            	output=new ByteArrayOutputStream();
            	up.writeTo(output);
                setValue2Redis(rowkey, output.toByteArray(),u_id,jedis);
            }
            pool.returnResourceObject(jedis);
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
        	if(output!=null)
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
        }
    }

    public static void main(String[] args){
    	
        if(args.length != 4){
        	logger.debug("Error: the number of args is "  + args.length);
            return;
        }
        DOMConfigurator.configure(args[2]);
        String password = "dmp123456";
        //DOMConfigurator.configure(args[2]);
        UpdateUserProfile inst = new UpdateUserProfile("192.168.40.20",6379, args[0], password);
        inst.scanResult2Redis(args[1], "up", "", args[3]);    
        logger.debug("Finish");
    }

}
