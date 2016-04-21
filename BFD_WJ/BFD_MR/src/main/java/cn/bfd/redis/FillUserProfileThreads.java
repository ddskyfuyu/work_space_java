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

import java.io.*;
import java.util.HashMap;
import java.util.Map;




/**
 * Created by yu.fu on 2015/4/18.
 */


public class FillUserProfileThreads {
	
	private final static Logger logger = Logger.getLogger(FillUserProfileThreads.class);
    //private RedisClient client;
	JedisPool pool  = null;
    private Configuration conf;
    private Map<String,String> map = new HashMap<String, String>();
    

    public FillUserProfileThreads(String ip, int port, String password){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxTotal(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(false);
        config.setTestWhileIdle(false);
        config.setTimeBetweenEvictionRunsMillis(1000*10*1000);
        pool = new JedisPool(config,ip, port, 1000*10, password);
    }
    
    public FillUserProfileThreads(String ip, int port, String password,String finFilter){
    	this(ip, port,password);
    	getFilterMap(finFilter);
    }
    
    

    
//    public boolean setValue2Redis(String key, String value, String bussine_id,Jedis jedis){
//    	String m_key = bussine_id + ":" + key;
//    	if(map.containsKey(m_key)){
//    	    jedis.set(bussine_id + ":" + key, value);
//    	    return true;
//    	}
//    	return false;
//    }
    
    public void setValue2Redis(String key, String value, String bussine_id,Jedis jedis){
	    jedis.set(bussine_id + ":" + key, value);
	    logger.debug("Successful M_key: " + key);
	    return;
    }
    
    public void setValue2Redis(String key, byte [] value, String bussine_id, Jedis jedis){
    	try {
			jedis.set((bussine_id + ":" + key).getBytes("utf-8"), value);
			logger.debug("Successful U_key: " + key);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }
    
    
    private void getFilterMap(String fin){
    	
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new FileReader(new File(fin)));
            String key="";
            while ((key=reader.readLine())!=null&&!"".equals(key)){
                map.put("M:"+key,"");
            }
            System.out.println("_____________" + map.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                reader.close();
            } catch (IOException e) {
            }
        }

    }

    
    
    public void scanResult2RedisWithRowKey(String table, String family, String column, String u_id){
    	ByteArrayOutputStream output=null;
        try {
            HTable hTable = new HTable(conf, Bytes.toBytes(table));
            Scan scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(100);
            ResultScanner reScanner = hTable.getScanner(scanner);
            Jedis jedis=pool.getResource();
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
                
                //ֱ�ӽ�userprofile���д��redis
            	output=new ByteArrayOutputStream();
            	up.writeTo(output);
                setValue2Redis(rowkey, output.toByteArray(),u_id,jedis);
            }
            //pool.returnResourceObject(jedis);
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
    
    
    
    public void scanResult2RedisWithRowKey(String table, String family, String column,
    		                               String m_id, String u_id){
    	ByteArrayOutputStream output=null;
        try {
            HTable hTable = new HTable(conf, Bytes.toBytes(table));
            Scan scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(100);
            ResultScanner reScanner = hTable.getScanner(scanner);
            
//            for(Result result : reScanner){
//            	Jedis jedis=pool.getResource();
//                String rowkey = new String(result.getRow());
//                logger.debug("Rowkey: " + rowkey);
//                PortraitOuterClass.Portrait up = null;
//                try{
//                	up = PortraitOuterClass.Portrait.parseFrom(result.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
//                }catch(IOException e){
//                	logger.error("rowkey: " + rowkey);
//                	e.printStackTrace();
//                	continue;
//                }
//                if(!up.hasUuid()){
//                    continue;
//                }
//                String[] keySet = up.getUuid().split(":");
//                if(keySet.length != 2){
//                    continue;
//                }
//                
//                //����mapping���д��userprofile
//                setValue2Redis(up.getUuid(), rowkey, m_id,jedis);
//                //д��G:key��ϵ
//            	output=new ByteArrayOutputStream();
//            	up.writeTo(output);
//                setValue2Redis(rowkey, output.toByteArray(),u_id,jedis);
//                pool.returnResourceObject(jedis);
//                
//            }
            //
//            jedis.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch(Exception e2){
        	e2.printStackTrace();
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

        //args[0]:mapping file, args[1]: table, args[2]
    	
        if(args.length != 8){
        	logger.debug("Usage: <log-confing> <ip> <port> <password> <u_id> <m_id> <table> <family>");
        	System.exit(-1);
        }
        String logConfig = args[0];
        String ip = args[1];
        int port = Integer.valueOf(args[2]);
        String password = args[3];
        String u_id = args[4];
        String m_id = args[5];
        String table = args[6];
        String family = args[7];
        String up = "";
        
        DOMConfigurator.configure(logConfig);
        
        //FillUserProfile inst = new FillUserProfile("192.168.40.20",6379, args[0], password);
        FillUserProfileThreads inst = new FillUserProfileThreads(ip, port, password);
        //FillUserProfile inst = new FillUserProfile(ip ,port, password);
        inst.scanResult2RedisWithRowKey(table, family, up, m_id, u_id);
        //inst.scanResult2RedisWithRowKey(table, family, up,u_id);
        //inst.scanResult2RedisWithRowKey(args[1], "up", "", args[3], args[4]);    
        logger.debug("Finish");
    }

}
