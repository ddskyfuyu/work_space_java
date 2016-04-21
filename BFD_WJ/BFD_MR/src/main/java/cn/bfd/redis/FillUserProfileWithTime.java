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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;




/**
 * Created by yu.fu on 2015/4/18.
 */


public class FillUserProfileWithTime {
	
	private final static Logger logger = Logger.getLogger(FillUserProfileWithTime.class);
    //private RedisClient client;
	JedisPool pool  = null;
    private Configuration conf;
    private Map<String,String> map = new HashMap<String, String>();
    private long startTime = 0l;
    private long endTime = 0l;
    private int thread_num = 0;
    

    public FillUserProfileWithTime(String ip, int port, String password){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        config.setMaxTotal(5);
        config.setMaxWaitMillis(1000);
        config.setTestOnBorrow(true);
        config.setTestWhileIdle(false);
        config.setTimeBetweenEvictionRunsMillis(1000*10*1000);
        pool = new JedisPool(config,ip, port, 1000*10, password);
    }
    
    public FillUserProfileWithTime(String ip, int port, String password,String finFilter){
    	this(ip, port,password);
    	getFilterMap(finFilter);
    }
    
    
    
    public FillUserProfileWithTime(String ip, int port, String password,long startTime, long endTime, int thread_num){
    	this(ip, port, password);
    	this.startTime = startTime;
    	this.endTime = endTime;
    	this.thread_num = thread_num;
    }
    
    public FillUserProfileWithTime(String ip, int port, String password,long startTime, long endTime){
    	this(ip, port, password);
    	this.startTime = startTime;
    	this.endTime = endTime;
    }
    
    public static void setValue2Redis(String key, String value, String bussine_id,Jedis jedis){
	    jedis.set(bussine_id + ":" + key, value);
	    logger.debug("Successful M_key: " + key);
	    return;
    }
    
    public static void setValue2Redis(String key, byte [] value, String bussine_id, Jedis jedis){
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
    
    
    public void splitScannerResult(String table, String family, String column, String m_id, String u_id){
        long time_interval = (endTime - startTime)/thread_num;
        logger.debug("########1:" + table);
        logger.debug("########2:" + time_interval);
        
        
        try {
			HTable hTable = new HTable(conf, Bytes.toBytes(table));
			ExecutorService executor=Executors.newCachedThreadPool();
			List<Future<Void>> futures=new ArrayList<Future<Void>>();
//    		Scan scanner = new Scan();
//            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
//            scanner.setTimeRange(startTime, endTime);
//            scanner.setBatch(0);
//            scanner.setCaching(100);
//            int resultNum = 0;
//            //�����Żؽ��
//            ResultScanner result_scanner = hTable.getScanner(scanner);
//			for(Result res : result_scanner){
//				resultNum++;
//				
//			}
//			logger.debug("################4 total: " + resultNum);
			
			
            
			//��Ƭ����
			int count = 0;
	    	for(long i = startTime; i <= endTime; i+=(time_interval + 1)){
	    		
	    		long minStamp = i;
	    		long maxStamp = i + time_interval;
	    		Scan scanner1 = new Scan();
	            scanner1.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
	            scanner1.setBatch(0);
	            scanner1.setCaching(100);
	            int resultNum = 0;
	            try {
	            	logger.debug("###########5"+ (++count) + ":[" + minStamp +"," + maxStamp + "]");
					scanner1.setTimeRange(minStamp, maxStamp);
//					scanResult2RedisWithRowKey(hTable, family, column, scanner, m_id, u_id);
					Jedis jedis=pool.getResource();
					ResultScanner result_scanner1 = hTable.getScanner(scanner1);
					
					for(Result res : result_scanner1){
						resultNum++;						
					}
					logger.debug("################6" + count + ":" + resultNum);
					futures.add(executor.submit(new JobWorker2(result_scanner1, jedis, family, column, m_id, u_id)));
					//pool.returnResourceObject(jedis);
//					new Thread(new JobWorker(hTable.getScanner(scanner), null)).start();
				} catch (IOException e) {
					e.printStackTrace();
				}
	    		
	    	}
	    	for(Future future:futures){
	    		try {
					future.get();
				} catch (Exception e) {
					e.printStackTrace();
				} 
	    	}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

    }
    
    public void sacanResultWithTime(String table, String family, String column, String m_id, String u_id){
    	ByteArrayOutputStream output=null;
        try {
        	HTable hTable = new HTable(conf, Bytes.toBytes(table));
    		Scan scanner = new Scan();
    		scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
    		scanner.setTimeRange(startTime, endTime);
    		scanner.setBatch(1);
    		scanner.setCaching(100);
            ResultScanner reScanner = hTable.getScanner(scanner);
            for(Result result : reScanner){
            	Jedis jedis=pool.getResource();
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
                //����mapping���д��userprofile
                setValue2Redis(up.getUuid(), rowkey, m_id, jedis);
                //д��G:key��ϵ
            	output=new ByteArrayOutputStream();
            	up.writeTo(output);
                setValue2Redis(rowkey, output.toByteArray(), u_id, jedis);
                pool.returnResourceObject(jedis);
            }
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
    
    public void scanResult2RedisWithRowKey(HTable hTable, String family, String column, Scan scanner, String m_id, String u_id){

    	ByteArrayOutputStream output=null;
        try {
        	
            ResultScanner reScanner = hTable.getScanner(scanner);
            for(Result result : reScanner){
            	Jedis jedis=pool.getResource();
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
                
                //����mapping���д��userprofile
                setValue2Redis(up.getUuid(), rowkey, m_id,jedis);
                //д��G:key��ϵ
            	output=new ByteArrayOutputStream();
            	up.writeTo(output);
                setValue2Redis(rowkey, output.toByteArray(),u_id,jedis);
                pool.returnResourceObject(jedis);
                
            }
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
            
            for(Result result : reScanner){
            	Jedis jedis=pool.getResource();
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
                
                //����mapping���д��userprofile
                setValue2Redis(up.getUuid(), rowkey, m_id,jedis);
                //д��G:key��ϵ
            	output=new ByteArrayOutputStream();
            	up.writeTo(output);
                setValue2Redis(rowkey, output.toByteArray(),u_id,jedis);
                pool.returnResourceObject(jedis);
                
            }
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
    	
        if(args.length != 11){
        	logger.debug("Usage: <log-confing> <ip> <port> <password> <u_id> <m_id> <table> <family> <starTime> <endTime> <num>");
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
        long startTime = Long.valueOf(args[8]);
        long endTime = Long.valueOf(args[9]);
        int thread_num = Integer.valueOf(args[10]);
        String up = "";
        DOMConfigurator.configure(logConfig);
        FillUserProfileWithTime inst = new FillUserProfileWithTime(ip, port, password, startTime, endTime);
        inst.sacanResultWithTime(table, family, up, m_id, u_id);
        logger.debug("Finish");
    }

    
    private static class JobWorker2 implements Callable<Void>{
    	private final static Logger loggerJob = Logger.getLogger(FillUserProfileWithTime.class);
    	private ResultScanner scanner;
    	private Jedis jedis;
    	private String family;
    	private String column;
    	private String m_id;
    	private String u_id;
    	
    	JobWorker2(ResultScanner scanner,Jedis jedis, String family, String column, String m_id, String u_id){
    		this.scanner=scanner;
    		this.jedis=jedis;
    		this.family = family;
    		this.column = column;
    		this.m_id = m_id;
    		this.u_id = u_id;
    	}

		public Void call() throws Exception {
			ByteArrayOutputStream output=new ByteArrayOutputStream();
			for(Result result:scanner){
				String rowkey = new String(result.getRow());
	            loggerJob.debug("Rowkey: " + rowkey);
	            PortraitOuterClass.Portrait up = null;
	            try{
	            	up = PortraitOuterClass.Portrait.parseFrom(result.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
	            }catch(IOException e){
	            	loggerJob.error("rowkey: " + rowkey);
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
	            
	            //����mapping���д��userprofile
	            setValue2Redis(up.getUuid(), rowkey, m_id,jedis);
	            //д��G:key��ϵ
	            up.writeTo(output);
	            setValue2Redis(rowkey, output.toByteArray(),u_id,jedis);
			}
			output.close();
            jedis.close();
			return null;
		}
    	
    }
    
}
