package cn.bfd.tools;

import cn.bfd.protobuf.UserProfile2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yu.fu on 2015/11/11.
 */

public class HbaseOperations {
    static Configuration conf = null;

    static{
        conf = HBaseConfiguration.create();
        /*
        conf.set("hbase.zookeeper.quorum", "192.168.49.203,192.168.49.204,192.168.49.205");
        conf.set("zookeeper.znode.parent", "/bfdhbasehot");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");
        */
        //测试集群的zk地址
        conf.set("hbase.zookeeper.quorum", "192.168.112.11,192.168.112.12,192.168.112.13");
        conf.set("zookeeper.znode.parent", "/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");
    }

    public static void getBulkFromHbase(String tableName,
                                        List<String> gidList,
                                        String Family,
                                        String column) throws IOException{

        //System.out.println("########## bulkImportHbase; The sourceTableName: " + sourceTableName + ", The desTableName:" + destinTableName);
        Connection connection = ConnectionFactory.createConnection(conf);
        Table sourceTable = connection.getTable(TableName.valueOf(tableName));
        //Table destineTable = connection.getTable(TableName.valueOf(destinTableName));
        //HTable sourceTable = new HTable(conf, sourceTableName);
        //HTable destineTable = new HTable(conf, destinTableName);
        //System.out.println("########################## 1 ########################");
        List<Get> gets = new ArrayList<Get>();
        for(int i = 0; i< gidList.size(); ++i){
            Get get = new Get(Bytes.toBytes(CommonUtils.reverseString(gidList.get(i))));
            get.addColumn(Bytes.toBytes(Family), Bytes.toBytes(column));
            gets.add(get);
        }
        try{
            Result[] results = sourceTable.get(gets);
            for(Result res : results){
                if(res == null || res.isEmpty()){
                    continue;
                }
                UserProfile2.UserProfile up = UserProfile2.UserProfile.parseFrom(res.getColumnLatest(Bytes.toBytes(Family), Bytes.toBytes(column)).getValue());
                if(up == null){
                    System.out.println("The protobuf failed.");
                    continue;
                }
                System.out.println("The successful gid is " + up.getUid());
            }
        }catch (IOException e){
            e.printStackTrace();
        }


        System.out.println("Get Operation Finish.");

    }

    public static void putResults(List<Get> gets,
                                  Table sourceTable,
                                  Table destinTable,
                                  String Family,
                                  String column){
        if(gets == null || gets.size() == 0){
            return;
        }
        List<Put> puts = new ArrayList<Put>();
        try{
            Result[] results = sourceTable.get(gets);
            for(Result res : results){
                //System.out.println("########################## 3 ########################");
                if(res == null || res.isEmpty()){
                    //System.out.println("############################ 5 Error #################################");
                    //System.out.println("########################### 5 result is empty. rowkey: " + new String(res.getRow()));
                    continue;
                }
                //System.out.println("################### 3, family: " + Family + ", column: " + column + ", rowkey: " + new String(res.getRow()));
                byte[] value = res.getColumnLatest(Bytes.toBytes(Family), Bytes.toBytes(column)).getValue();
                Put put = new Put(res.getRow());
                //System.out.println("####### 3: " + new String(value));
                put.add(Bytes.toBytes(Family), Bytes.toBytes(column), value);
                puts.add(put);
            }
            //System.out.println("########################## 4 ########################");
            //System.out.println("########## the size of puts is " + puts.size());
            destinTable.put(puts);
            destinTable.flushCommits();
            //System.out.println("########################## 4 finish ########################");
        }catch(IOException e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException{
        if(args.length != 4){
            System.out.println("<Usage>: Source_Table, Family, Column, Gid_File");
            return;
        }

        String gidFileName = args[3];
        String sourceTableName = args[0];
        String family = args[1];
        String column = null;

        //System.out.println("The sourceTableName: " + sourceTableName + ", The desTableName:" + desTableName);
        if("empty".equals(args[2])){
            column = "";
        }
        else{
            column = args[2];
        }

        if(column == null){
            System.out.println("The column is null");
            return;
        }

        //int patchNum = Integer.valueOf(args[5]);


        List<String> gidList = new ArrayList<String>();
        CommonUtils.FillArrayList(gidList, gidFileName, ",", 1, 0);

        System.out.println("gidList size: " + gidList.size());


        HbaseOperations.getBulkFromHbase(sourceTableName, gidList, family, column);

    }






}
