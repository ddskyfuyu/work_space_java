package cn.bfd.redis;

import cn.bfd.protobuf.PortraitOuterClass;
import com.bfd.dqp.jedis.JedisClient;
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


public class FillHwUpToRedis {
	
	private final static Logger logger = Logger.getLogger(FillHwUpToRedis.class);
	private String ghName;
	private String uhName;
	private String ufileName;
	private String gfileName;
	JedisPool pool  = null;
    private Configuration conf;
    private Map<String,String> map = new HashMap<String, String>();
    private Map<String, String> gmap = new HashMap<String, String>();
    private Map<String, String> umap = new HashMap<String, String>();
    

    public FillHwUpToRedis(String ip, int port, String password){
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
    
    
    public FillHwUpToRedis(){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
    }
    
    
    public FillHwUpToRedis(String gbName, String uhName, String ufileName, String gfileName){
    	this();
    	this.ghName = gbName;
    	this.gfileName = gfileName;
    	this.uhName = uhName;
    	this.gfileName = gfileName;
    	getFilterMap(gfileName, gmap);
    	getFilterMap(ufileName, umap);
    	
    }
    
      
    public FillHwUpToRedis(String ip, int port, String password,String finFilter){
    	this(ip, port,password);
    	getFilterMap(finFilter);
    }
    
    
    
    public void setValue2Redis(String key, String value, String bussine_id,Jedis jedis){
    	jedis.set(bussine_id + ":" + key, value);     	
	    logger.debug("Successful M_key: " + key);
	    return;
    }
    
    public void setValue2Redis(String key, String value, String bussine_id){
    	JedisClient.set(bussine_id + ":" + key, value);
	    logger.debug("Successful M_key: " + key);
	    return;
    }
    
    public void setValue2Redis(String key, byte [] value, String bussine_id, Jedis jedis){
    	try {
			JedisClient.set((bussine_id + ":" + key).getBytes("utf-8"), value);
			logger.debug("Successful U_key: " + key);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
    }
    
    public void setValue2Redis(String key, byte [] value, String bussine_id){
    	try {
			JedisClient.set((bussine_id + ":" + key).getBytes("utf-8"), value);
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
            	if(key.split("\t").length != 2){
            		continue;
            	}
                map.put(key.split("\t")[0],"");
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
    
    
    private void getFilterMap(String fin, Map<String, String> filter){
    	
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new FileReader(new File(fin)));
            String key="";
            while ((key=reader.readLine())!=null && !"".equals(key)){ 	
                map.put(key,"");
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
    

    
    
    public void scanResult2RedisWithRowKey(String family, String column,
    		                               String m_id, String u_id){
    	ByteArrayOutputStream output=null;
    	HTable hTable = null;
    	Scan scanner = null;
    	ResultScanner reScanner = null;
    	
        try {
            hTable = new HTable(conf, Bytes.toBytes(uhName));
            scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(100);
            reScanner = hTable.getScanner(scanner);
            for(Result result : reScanner){
                String rowkey = new String(result.getRow());
                PortraitOuterClass.Portrait up = null;
                try{
                	up = PortraitOuterClass.Portrait.parseFrom(result.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
                }catch(IOException e){
                	logger.error("Error rowkey: " + rowkey);
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
                if(umap.containsKey(rowkey)){
                	//����mapping���д��userprofile
                	setValue2Redis(up.getUuid(), rowkey, m_id);
                    //д��G:key��ϵ
                	output=new ByteArrayOutputStream();
                	up.writeTo(output);
                    setValue2Redis(rowkey, output.toByteArray(),u_id);
                }
            }
            
            //��G��д�뵽redis
            hTable = new HTable(conf, Bytes.toBytes(ghName));
            scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(100);
            reScanner = hTable.getScanner(scanner);
            //������ص�G:bfdgid�ؼ���
            String key = "G:bfdgid";
            JedisPool pool = JedisClient.getPool(key);
            Jedis jedis=pool.getResource();
            for(Result result : reScanner){
                String rowkey = new String(result.getRow());
                PortraitOuterClass.Portrait up = null;
                try{
                	up = PortraitOuterClass.Portrait.parseFrom(result.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
                }catch(IOException e){
                	logger.error("G Error rowkey: " + rowkey);
                	e.printStackTrace();
                	continue;
                }
                if(gmap.containsKey(rowkey)){
                    //��G�����ڷ���
                    if(JedisClient.get((u_id + ":" + rowkey).getBytes("utf-8")) != null){
                    	logger.debug("Filter Rowkey: " + rowkey);
                        continue;
                    }
                    jedis.sadd(key, rowkey); 
                    //д��redis, keyΪU:rowkey, valueΪ�û�����
                	output = new ByteArrayOutputStream();
                	up.writeTo(output);
                    setValue2Redis(rowkey, output.toByteArray(),u_id);
                }
            }
            pool.returnBrokenResource(jedis);
            
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
        if(args.length != 8){
        	logger.debug("Usage: <log-confing> <g_filter_path> <u_filter_path> <g_table> <u_table> <u_id> <m_id> <family>");
        	System.exit(-1);
        }
        String logConfig = args[0];
        String u_map_path = args[1];
        String g_map_path = args[2];
        String g_table = args[3];
        String u_table = args[4];
        String u_id = args[5];
        String m_id = args[6];
        String family = args[7];
        String up = "";
        DOMConfigurator.configure(logConfig);
        FillHwUpToRedis inst = new FillHwUpToRedis(g_table, u_table, u_map_path, g_map_path);
        inst.scanResult2RedisWithRowKey(family, up, m_id, u_id);    
        logger.debug("Finish");
    }

}
