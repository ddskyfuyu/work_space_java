package cn.bfd.project.tcl;

import cn.bfd.protobuf.PortraitOuterClass;
import com.bfd.dqp.jedis.JedisClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by yu.fu on 2015/11/7.
 */

public class TclUpService {

    private static Configuration conf;
    private final static Logger logger = Logger.getLogger(TclUpService.class);

    static{
        conf = HBaseConfiguration.create();
        //线上集群的zk地址
        conf.set("hbase.zookeeper.quorum", "192.168.49.203,192.168.49.204,192.168.49.205");
        conf.set("zookeeper.znode.parent", "/bfdhbasehot");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");
    }

    /*
     * 将key作为redis的key, up作为redis的value, key和up都是字节形式存储的
     * @param: up, 用户画像protobuffer对象
     * @param: key, U域的值, U:rowkey
     * @return: boolean, true表示set成功; false表示set失败
    */
    public boolean set2Udomain(PortraitOuterClass.Portrait up, String rowkey) {
        if(up == null){
            return false;
        }

        if(!up.hasUuid()){
            return false;
        }

        try{
            String key = "U:" + rowkey;
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            up.writeTo(output);
            JedisClient.set(key.getBytes("utf-8"), output.toByteArray());
        }catch(IOException e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /*
     * 将key作为redis的key, value作为redis的value, key和up都是字符串形式存储的,
     * @param: uid, 用户画像的uuid
     * @param: rowkey, 用户画像的存储在Hbase中的rowkey
     * @return: boolean, true表示set成功; false表示set失败
     */
    public boolean set2Mdomain(String uid, String rowkey) {
        if(uid == null || rowkey == null){
            return false;
        }
        String key = "M:" + uid;
        JedisClient.set(key, rowkey);
        return true;
    }

    /*
     * 查找指定key是否存储在M域中
     * @param: uid, 用户画像的uuid，作为M域的key
     * @return: boolean, true表示存在; false表示不存在
    */
    public boolean isExistMdomain(String uid){
        if(uid == null){
            return false;
        }

        String key = "M:" + uid;
        String value = JedisClient.get(key);

        boolean res = value != null ? true:false;
        System.out.println("key: " + key + " is exists; value: " + value);
        return res;
    }

    /*
     * 扫描整个Hbase表，将全量表的数据更新到redis中
     * @param: table, hbase表名
     * @param: family, hbase的列簇名
     * @param: column, hbase的列名
     * @return: void
     */
    public void scanResult2Redis(String table,String family, String column){

        ByteArrayOutputStream output = null;
        Table hTable = null;
        Scan scanner = null;
        ResultScanner reScanner = null;

        try {
            //使用Connection方式声Hbase表对象，
            Connection connection = ConnectionFactory.createConnection(conf);
            hTable = connection.getTable(TableName.valueOf(table));

            //扫描整个表，获取指定列的内容
            scanner = new Scan();
            scanner.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            scanner.setBatch(0);
            scanner.setCaching(1000);
            reScanner = hTable.getScanner(scanner);

            for(Result result : reScanner){
                if(result.isEmpty() || result == null){
                    continue;
                }

                String rowkey = new String(result.getRow());
                //DEBUG
                System.out.println("###############1: read rowkey: " + rowkey);

                PortraitOuterClass.Portrait up = null;
                try{
                    up = PortraitOuterClass.Portrait.parseFrom(result.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
                }catch(IOException e){
                    //DEBUG
                    System.out.println("###############2: parse userprofile failed: " + rowkey);
                    //logger.error("Error rowkey: " + rowkey);
                    e.printStackTrace();
                    continue;
                }

                if(!up.hasUuid()){
                    //DEBUG
                    System.out.println("###############3: up does not have uuid attribute: " + rowkey);
                    continue;
                }
                //判断M区是否存在, M区构成：M:uid, uid是用户画像的中的uid
                if(isExistMdomain(up.getUuid())){

                    //M区域关键字
                    String key = "M:" + up.getUuid();
                    //DEBUG
                    System.out.println("###############4: M Domain key: " + key);
                    //提取对应的rowkey值，更新U域的画像
                    String value = JedisClient.get(key);
                    //DEBUG
                    System.out.println("###############4: M Domain value: " + value);
                    if(!set2Udomain(up,value)){
                        System.out.println("##################### 6: The rowkey:  update U domain failed. " + value);
                    }
                    //DEBUG
                    System.out.println("##################### 6: The rowkey:  update U domain successful." + value);

                }
                else{
                    //DEBUG
                    System.out.println("###############5: update U and M domain ###############");
                    if(!set2Mdomain(up.getUuid(), rowkey)){
                        System.out.println("##################### 5: The rowkey:  update M domain failed. " + rowkey);
                    }
                    //DEBUG
                    System.out.println("##################### 5: The rowkey:  update M domain successful. " + rowkey);

                    if(!set2Udomain(up, rowkey)){
                        System.out.println("##################### 5: The rowkey:  update M domain failed. " + rowkey);
                    }
                    //DEBUG
                    System.out.println("##################### 5: The rowkey:  update U domain successful. " + rowkey);
                }


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


    public static void main(String[] args){
        if(args.length != 3){
            System.out.println("<Usage> table family column");
            return;
        }

        String tableName = args[0];
        String family = args[1];
        String column = null;

        //针对列名为空的现象，如果参数是empty字符串，则认为列名为空
        if("empty".equals(args[2])){
            column = "";
        }
        else{
            column = args[2];
        }

        //调用scanResult2Redis函数将tableName中指定的Hbase表同步到redis中
        TclUpService tclUpService = new TclUpService();
        tclUpService.scanResult2Redis(tableName, family, column);

//        String value = JedisClient.get("fy_key_test");
//        if(value == null){
//            System.out.println("The value return null");
//        }
//        else{
//            System.out.println("The value return: " + value);
//        }
//
//        JedisClient.set("fy_key_test", "fy_key_value");
//        System.out.println("The fy_key_test value: " + JedisClient.get("fy_key_test"));
    }





}
