package cn.bfd.mr;

import cn.bfd.protobuf.UserProfile2;
import com.google.protobuf.InvalidProtocolBufferException;
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


public class GetUpFromScan {
	
    
     public static class MyMapper extends TableMapper<Text, Text>{
    	 private String family = new String();
    	 private String column = new String();
    	 
    	 protected void setup(Context context) throws IOException,InterruptedException {
    		 Configuration conf = context.getConfiguration();
    		 family = conf.get("FAMILY", "-1");
             column = conf.get("COLUMN", "");
    	 }
         
         public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException{
        	 if(val.isEmpty()){
        		 return;
             } 
             String rowKey = new String(row.get());
             if(rowKey.isEmpty()){
            	   return;
             }
             UserProfile2.UserProfile up = null;
             try{
             	up = UserProfile2.UserProfile.parseFrom(val.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
             }catch(InvalidProtocolBufferException e){
             	System.out.println("RowKey: " + rowKey);
             	return;
             }
             if(up.getCidInfoCount() > 0){

            	 UserProfile2.CidInfo cidInfo = null;
 	        	int index = 0;
 	        	for(; index < up.getCidInfoCount(); ++index){
 	        		cidInfo = up.getCidInfo(index);
             		if(cidInfo.getCid().equals("Cbaifendian")){
             			break;
             		} 
             	}
 	        	if(index != up.getCidInfoCount()){
 	        		if(cidInfo.getIndusCount() != 0){
 	        			UserProfile2.IndustryInfo indusInfo = null;
 	        			for(int i = 0; i < cidInfo.getIndusCount(); ++i){
          				   indusInfo = cidInfo.getIndus(i);
          				   if(indusInfo.getFirstCateCount() != 0){
          					   //����Ʒ��
//              				   for(int findex= 0; findex < indusInfo.getFirstCateCount(); ++findex){
//              					   if(indusInfo.getFirstCate(findex).getName().equals("��װ����")){
//              						 context.write(new Text(up.getUid()), new Text(String.valueOf(up.toString().length())));
//            				   }
//              					   return;
//              				   }
              				   //ý��Ʒ��
              				   for(int findex= 0; findex < indusInfo.getMediaCateCount(); ++findex){
              					   if(indusInfo.getMediaCate(findex).getName().equals("�ƾ�")){
              						 context.write(new Text(up.getUid()), new Text(String.valueOf(up.toString().length())));
              					   }
              					   return;
              				   }
          				   }
 	        		    }
 	        		}
 	        	}
 		
             }
               
          }
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
    	  Configuration conf = HBaseConfiguration.create();
          //set parameters
          String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
          if(otherArgs.length != 3){
        	  System.out.println("Usage: <table> <family> <output>");
        	  System.exit(-1);
          }
          conf.set("Table", otherArgs[0]);
          conf.set("FAMILY", otherArgs[1]);
          conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
          conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
          conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
          Job job = new Job(conf, "GetUpWithCategory");
          job.setJarByClass(GetUpFromScan.class);
          
          //DEBUG_B
          for(int i = 0; i < otherArgs.length; ++i){
        	  System.out.println("The " + i +" argments is " + otherArgs[i]);
          }
          //DEBUG_E
          Scan scan = new Scan();
          scan.addFamily(Bytes.toBytes(otherArgs[1]));
          scan.setCaching(1000);
          scan.setCacheBlocks(false);
          TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(otherArgs[0]), scan, MyMapper.class, Text.class, Text.class, job);
          job.setNumReduceTasks(0);
          FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); 
          System.exit(job.waitForCompletion(true) ? 0 : 1);
         
     }

}
