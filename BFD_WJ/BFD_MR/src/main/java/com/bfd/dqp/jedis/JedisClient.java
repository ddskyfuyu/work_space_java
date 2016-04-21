package com.bfd.dqp.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.UnsupportedEncodingException;

/**
 * Created by zheng.liu@baifendian on 2015-05-26.
 */
public class JedisClient {
    public static String get(String key) {
        JedisShard.Node node = JedisShard.jedisShard.keyToNode(key);
        String rs = "";
//        System.out.println("key :"+key+"---"+ node.toString());
        Jedis jedis = node.getPool().getResource();
        rs = jedis.get(key);
        node.getPool().returnResource(jedis);
        return rs;
    }

    public static void set(String key, String value) {
        JedisShard.Node node = null;
        try {
            node = JedisShard.jedisShard.keyToNode(key);
            Jedis jedis = node.getPool().getResource();
            jedis.set(key, value);
            node.getPool().returnResource(jedis);
        } catch (Exception e) {
            e.printStackTrace();

        }

    }

    public static byte[] get(byte[] key) {
        JedisShard.Node node = null;
        try {
            node = JedisShard.jedisShard.keyToNode(new String(key, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        byte[] rs = null;
        Jedis jedis = node.getPool().getResource();
        rs = jedis.get(key);
        node.getPool().returnResource(jedis);
        return rs;
    }

    public static void set(byte[] key, byte[] value) {
        JedisShard.Node node = null;
        try {
            node = JedisShard.jedisShard.keyToNode(new String(key, "UTF-8"));
            Jedis jedis = node.getPool().getResource();
            jedis.set(key, value);
            node.getPool().returnResource(jedis);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(key + "\t" + (node == null));
        }

    }

    /**
     * 这个方法是根据Key获得Redis的连接池，同时在池中获得的连接使用完成后要返回
     * @param key
     * @return
     */
    public static JedisPool getPool(String key) {
        JedisShard.Node node = null;
        node = JedisShard.jedisShard.keyToNode(key);
        return node.getPool();
    }

    /**
     * 这个方法是根据Key获得Redis的连接池，同时在池中获得的连接使用完成后要返回
     * @param key
     * @return
     */
    public static JedisPool getPool(byte [] key) {
        JedisShard.Node node = null;
        try {
            node = JedisShard.jedisShard.keyToNode(new String(key,"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return node.getPool();
    }


}
