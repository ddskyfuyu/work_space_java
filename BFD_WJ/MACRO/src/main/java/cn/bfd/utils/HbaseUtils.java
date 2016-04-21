package cn.bfd.utils;

import org.apache.hadoop.hbase.thrift2.generated.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用Thrift2接口定义相关的操作
 * Created by yu.fu on 2015/12/9.
 */
public class HbaseUtils {

    private final String hbase_table;
    private final String hbase_thrift_ip;
    private final int hbase_thrift_port;
    private final TTransport transport;
    private final THBaseService.Iface hbase_client;


    /**
     * HbaseUtils的结构体函数
     *
     * @param hbase_table hbase的表名
     * @param hbase_thrift_ip hbase thrift对应的IP
     * @param hbase_thrift_port hbase thrift对应的端口号
     */
    public HbaseUtils(String hbase_table, String hbase_thrift_ip, int hbase_thrift_port){
        this.hbase_table = hbase_table;
        this.hbase_thrift_ip = hbase_thrift_ip;
        this.hbase_thrift_port = hbase_thrift_port;

        //使用Thrift建立Hbase client
        transport = new TSocket(hbase_thrift_ip, hbase_thrift_port);
        TProtocol protocol = new TBinaryProtocol(transport);
        hbase_client = new THBaseService.Client(protocol);
    }



    /**
     * 根据gids, 从Hbase中批量获取列名不为空的用户画像记录
     * @param gids 字符串列表 存放gid的字符串列表
     * @param family 字符串 存放列簇的名称
     * @param column 字符串 存放列的名称
     * @return TGet类型的列表 返回设置好的TGet类型的列表
     */
    public List<TGet> getGets(List<String> gids, String family, String column ){
        List<TGet> gets = new ArrayList<TGet>();

        for (String gid : gids) {
            if (gid == null || gid.isEmpty()){
                continue;
            }
            //利用Thrift接口，批量产生Get，存放到对应的列表中
            TGet g = new TGet();
            //Hbase中存储的rowkey是gid的逆序
            String rowkey = UpUtils.reverseString(gid);
            g.setRow(rowkey.getBytes());
            TColumn t = new TColumn();
            t.setFamily(family.getBytes());
            //判断列名是否为空
            if (column != null && column.trim().length() > 0) {
                t.setQualifier(column.getBytes());
            }else{
                t.setQualifier(Bytes.toBytes(""));
            }
            g.addToColumns(t);
            gets.add(g);
        }
        return gets;
    }


    public TGet getGet(String gid, String family, String column){

        //利用Thrift接口,产生TGet
        TGet g = new TGet();
        String rowkey = UpUtils.reverseString(gid);

        g.setRow(rowkey.getBytes());
        TColumn t = new TColumn();
        t.setFamily(family.getBytes());
        t.setQualifier(column.getBytes());

        g.addToColumns(t);

        return g;
    }



    public TGet getMuliColumnGet(String gid, String family, List<String> columns){
        //利用Thrift接口,产生TGet
        TGet g = new TGet();
        String rowkey = UpUtils.reverseString(gid);
        System.out.println("###################### 8 rowkey: " + rowkey);
        g.setRow(rowkey.getBytes());
        for(String column : columns){
            TColumn t = new TColumn();
            t.setFamily(family.getBytes());
            t.setQualifier(column.getBytes());
            g.addToColumns(t);
        }

        return g;
    }

    public TGet getMuliColumnGet(String gid, String family, String[] columns){
        //利用Thrift接口,产生TGet
        TGet g = new TGet();
        String rowkey = UpUtils.reverseString(gid);

        g.setRow(rowkey.getBytes());
        for(String column : columns){
            TColumn t = new TColumn();
            t.setFamily(family.getBytes());
            t.setQualifier(column.getBytes());
            g.addToColumns(t);
        }
        return g;
    }

    public List<TGet> getPatchMuliColumnGet(String[] gids, String family, List<String> columns){
        List<TGet> gets = new ArrayList<TGet>();

        for(String gid : gids){
            gets.add(getMuliColumnGet(gid, family, columns));
        }
        return gets;
    }

    public List<TGet> getPatchMuliColumnGet(List<String> gids, String family, List<String> columns){
        List<TGet> gets = new ArrayList<TGet>();

        for(String gid : gids){
            gets.add(getMuliColumnGet(gid, family, columns));
        }
        return gets;
    }

    public List<TResult> getResults(List<TGet> gets){
        //thrift接口批量获取TResult
        List<TResult> results = null;
        try{
            transport.open();
            ByteBuffer table = ByteBuffer.wrap(hbase_table.getBytes());
            results = hbase_client.getMultiple(table, gets);
            //return results;
        }catch(TTransportException e){
            e.printStackTrace();
        }catch(TIOError e){
            e.printStackTrace();
        }catch(TException e){
            e.printStackTrace();
        }
        finally {
            transport.close();
            return results;
        }
    }


    public List<TResult> getResults(List<String> gids, String family, List<String> columns){

        List<TGet> gets = new ArrayList<TGet>();
        for(String gid : gids){
            gets.add(getMuliColumnGet(gid, family, columns));
        }
        List<TResult> results = null;
        //thrift接口批量获取TResult
        try{
            transport.open();
            ByteBuffer table = ByteBuffer.wrap(hbase_table.getBytes());
            results = hbase_client.getMultiple(table, gets);
            //return results;
        }catch(TTransportException e){
            e.printStackTrace();
        }catch(TIOError e){
            e.printStackTrace();
        }catch(TException e){
            e.printStackTrace();
        }
        finally {
            transport.close();
            return results;
        }
    }

    public List<TResult> getResults(List<String> gids, String family, String[] columns){

        //DEBUG
        List<TGet> gets = new ArrayList<TGet>();
        for(String gid : gids){
            //System.out.println("################ 1 ######### The input gid: " + gid);
            gets.add(getMuliColumnGet(gid, family, columns));
        }

        List<TResult> results = null;
        //System.out.println("################ 2 ######### ");
        //thrift接口批量获取TResult
        try{
            //System.out.println("################ 3 ######### ");
            transport.open();
            ByteBuffer table = ByteBuffer.wrap(hbase_table.getBytes());
            results = hbase_client.getMultiple(table, gets);
            //System.out.println("################ 4 ######### ");
            //return results;
        }catch(TTransportException e){
            //System.out.println("################ 5 ######### ");
            e.printStackTrace();
        }catch(TIOError e){
            //System.out.println("################ 6 ######### ");
            e.printStackTrace();
        }catch(TException e){
            //System.out.println("################ 7 ######### ");
            e.printStackTrace();
        }
        finally {
            transport.close();
            return results;
        }
    }


    public static void main(String[] args){
        if(args.length != 4){
            System.out.println("<Usage> table thrift_ip thrif_port input_path");
            System.exit(1);
        }

        String table = args[0];
        String thrift_ip = args[1];
        int thrift_port = Integer.valueOf(args[2]);
        String input_path = args[3];

        //System.out.println("table: " + table + ", thrift_ip: " + thrift_ip + "thrift_port: "  + thrift_port);


        List<String> gids = new ArrayList<String>();
        FileUtils.FillList(gids, input_path);

        HbaseUtils hbaseUtils = new HbaseUtils(table,thrift_ip, thrift_port);

        String[] columns = {"demographic","market", "inet_PC", "inet_Mobile"};

        List<TResult> results = hbaseUtils.getResults(gids, "up", columns);
        if(results == null){
            //System.out.println("###################### 9 ################## results is null ############");
            return;
        }
        for(TResult res : results){
            if(res == null){
                //System.out.println("###################### 8 ################## res is null ############");
                continue;
            }
            for(TColumnValue resValue: res.getColumnValues()){
                System.out.print(",family = " + new String(resValue.getFamily()));
                System.out.print(",qualifier = " + new String(resValue.getQualifier()));
                System.out.print(",value = " + new String(resValue.getValue()));
                System.out.print(",timestamp = " + resValue.getTimestamp());
            }

        }
    }
}
