package cn.bfd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class ScanGetRowKey {
	
	
	public static class MyMapper extends TableMapper<Text, Text>{
	
	public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException{
        if(val.isEmpty()){
            return;
       } 
       String rowKey = new String(row.get());
       if(rowKey.isEmpty()){
    	   return;
       }
       
       context.write(new Text(rowKey), null);
	}
	
	}

	public static void main(String[] args) throws Exception{
  	  Configuration conf = HBaseConfiguration.create();
      //set parameters
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
      if(otherArgs.length != 2){
   	   System.out.println("The number of parameters is wrong.");
   	   return;
      }
      conf.set("Table", otherArgs[0]);
      //conf.set("mapred.textoutputformat.separator", ","); 
      conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
      conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
      conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
      Job job = new Job(conf, "HbaseScan");
      job.setJarByClass(ScanGetRowKey.class);
      //DEBUG_B
      for(int i = 0; i < otherArgs.length; ++i){
    	  System.out.println("The " + i +" argments is " + otherArgs[i]);
      }
      //DEBUG_E
      Scan scan = new Scan();
      //scan.addFamily(Bytes.toBytes(otherArgs[1]));
      //scan.addColumn(Bytes.toBytes(otherArgs[1]), Bytes.toBytes(column));
      scan.setCaching(1000);
      scan.setCacheBlocks(false);
      TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(otherArgs[0]), scan, MyMapper.class, Text.class, Text.class, job);
      job.setNumReduceTasks(0);
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
      System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
