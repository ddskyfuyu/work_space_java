package cn.bfd.redis;

import com.bfd.dqp.jedis.JedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;





public class RedisOp {
	
	public Map<String, String> strMap = new HashMap<String, String>();
	
	
	public void FillMap(String fin){
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new FileReader(new File(fin)));
            String key="";
            while ((key=reader.readLine())!=null && !"".equals(key)){
            	String[] elems = key.split("\t");
            	if(elems.length != 2){
            		continue;
            	}
            	
                strMap.put(elems[0], elems[1]);
            }
            System.out.println("_____________" + strMap.size());
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
	
	public void save2redis(String key) throws UnsupportedEncodingException{

        //������ص�G:bfdgid�ؼ���
        //String key = "G:bfdgid";
        JedisPool pool = JedisClient.getPool(key);
        Jedis jedis=pool.getResource();
        for(Map.Entry<String, String> entry : strMap.entrySet()){
        	String rowkey = entry.getKey();
            //��G�����ڷ���
            if(JedisClient.get(("U:" + rowkey).getBytes("utf-8")) != null){
            	System.out.println("Rowkey: " + rowkey);
            	continue;
            }
            jedis.sadd(key, rowkey); 
            JedisClient.set(("U:" + rowkey).getBytes("utf-8"), entry.getValue().getBytes("utf-8"));
        }
        pool.returnBrokenResource(jedis);
	}
	
	public void showSetFromRedis(String key) throws UnsupportedEncodingException{
		//WString key = "G:bfdgid";
        JedisPool pool = JedisClient.getPool(key);
        Jedis jedis = pool.getResource();
        Set set = jedis.smembers(key); 
        Iterator<String> t1= set.iterator() ; 
        while(t1.hasNext()){ 
            Object obj1=t1.next(); 
            System.out.println("Set: " + obj1);
            if(JedisClient.get(("U:" + obj1).getBytes("utf-8")) != null){
            	System.out.println("U domain: " + obj1 + "," + JedisClient.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             get("U:" + obj1));
            }
        }
        pool.returnBrokenResource(jedis);
	}
	
	
	public RedisOp(String fin){
		FillMap(fin);
	}
	
	public static void main(String[] args) throws UnsupportedEncodingException{
		if(args.length != 2){
			System.out.println("Usege: <fin> <u-id>");
			System.exit(1);
		}
		String fin = args[0];
		RedisOp ins = new RedisOp(fin);
		ins.save2redis(args[1]);
		ins.showSetFromRedis(args[1]);
	}
}
