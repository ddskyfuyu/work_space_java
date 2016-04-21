package cn.bfd.tools;

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

public class BulkImportHbase {
    static Configuration conf = null;

    static{
        conf = HBaseConfiguration.create();
        //线上集群的zk地址
        conf.set("hbase.zookeeper.quorum", "192.168.49.203,192.168.49.204,192.168.49.205");
        conf.set("zookeeper.znode.parent", "/bfdhbasehot");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");

        //测试集群的zk地址
        /*
        conf.set("hbase.zookeeper.quorum", "192.168.112.11,192.168.112.12,192.168.112.13");
        conf.set("zookeeper.znode.parent", "/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");
        */
    }

    public static void bulkImportHbase(String sourceTableName,
                                       String destinTableName,
                                       List<String> gidList,
                                       String Family,
                                       String column,
                                       int patch_num) throws IOException{

        Connection connection = ConnectionFactory.createConnection(conf);
        Table sourceTable = connection.getTable(TableName.valueOf(sourceTableName));
        Table destineTable = connection.getTable(TableName.valueOf(destinTableName));

        List<Get> gets = new ArrayList<Get>();
        int count = 0;
        for(int i = 0; i< gidList.size(); ++i){
            Get get = new Get(Bytes.toBytes(CommonUtils.reverseString(gidList.get(i))));
            get.addColumn(Bytes.toBytes(Family), Bytes.toBytes(column));
            gets.add(get);
            count++;
            if(count == patch_num){
                putResults(gets, sourceTable, destineTable, Family, column);
                count = 0;
                gets.clear();
            }
        }

        if(count != 0){
            putResults(gets, sourceTable, destineTable, Family, column);
        }
        System.out.println("Import Finish.");

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
                if(res == null || res.isEmpty()){
                    continue;
                }
                byte[] value = res.getColumnLatest(Bytes.toBytes(Family), Bytes.toBytes(column)).getValue();
                Put put = new Put(res.getRow());
                put.add(Bytes.toBytes(Family), Bytes.toBytes(column), value);
                puts.add(put);
            }

            //批量导入数据
            destinTable.put(puts);
            destinTable.flushCommits();

        }catch(IOException e){
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException{
        if(args.length != 6){
            System.out.println("<Usage>: Source_Table, Des_Table, Family, Column, Gid_File, Patch_Num");
            return;
        }

        String gidFileName = args[4];
        String sourceTableName = args[0];
        String desTableName = args[1];
        String family = args[2];
        String column = null;

        System.out.println("The sourceTableName: " + sourceTableName + ", The desTableName:" + desTableName);
        if("empty".equals(args[3])){
            column = "";
        }
        else{
            column = args[3];
        }

        if(column == null){
            System.out.println("The column is null");
            return;
        }

        int patchNum = Integer.valueOf(args[5]);


        List<String> gidList = new ArrayList<String>();
        CommonUtils.FillArrayList(gidList, gidFileName, ",", 1, 0);

        System.out.println("gidList size: " + gidList.size());


        BulkImportHbase.bulkImportHbase(sourceTableName, desTableName, gidList, family, column, patchNum);



    }






}
