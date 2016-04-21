package cn.bfd.mr;

import cn.bfd.protobuf.UserProfile2;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class getDetailUpFromScan {
	
    
     public static class MyMapper extends TableMapper<Text, Text>{
    	 
    	 private String family = new String();
    	 private String column = new String();
    	 public Map<String, Integer> filterMap = new HashMap<String, Integer>();
    	 
 		public String generateRowKey(String str) {
			char c[] = str.toCharArray();
			char t;
			for (int i = 0; i < (str.length() + 1) / 2; i++) {
				t = c[i];
				c[i] = c[c.length - i - 1];
				c[c.length - i - 1] = t;
			}
			String str2 = new String(c);
			return str2;
		}
    	 
    	 protected void setup(Context context) throws IOException,InterruptedException {
    		 Configuration conf = context.getConfiguration();
    		 family = conf.get("FAMILY", "-1");
             column = conf.get("COLUMN", "");
             
             Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
             String finName = null;
             finName = localFiles[0].toString();
             BufferedReader attr_reader = new BufferedReader(new FileReader(finName));
             String line = "";
             
             while (((line = attr_reader.readLine()) != null)) {
 				if (line.trim().isEmpty() == true) {
 					continue;
 				}
 				String gid = line.trim();
 				filterMap.put(gid, 1);
 			}
    	 }
    	 
    	 static <K,V extends Comparable<? super V>>
    	 SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
    	     SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
    	         new Comparator<Map.Entry<K,V>>() {
    	             public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
    	                 int res = e1.getValue().compareTo(e2.getValue());
    	                 return res != 0 ? -res : 1;
    	             }
    	         }
    	     );
    	     sortedEntries.addAll(map.entrySet());
    	     return sortedEntries;
    	 }
    	   	 
         public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException{
        	 if(val.isEmpty()){
        		 return;
             } 
             String rowKey = new String(row.get());
             if(rowKey.isEmpty()){
            	   return;
             }
             
             if(family.equals("-1") || column.equals("-1")){
            	 System.out.println("FAMILY, COLUMN key is wrong");
            	 return;
             }
             
             if(!filterMap.containsKey(generateRowKey(rowKey))){
            	 return;
             }
             
             StringBuffer buffer = new StringBuffer();
             UserProfile2.UserProfile up = null;
             try{
          	   up = UserProfile2.UserProfile.parseFrom(val.getColumnLatest(Bytes.toBytes(family),Bytes.toBytes(column)).getValue());
             }catch(InvalidProtocolBufferException e){
          	   System.out.println("RowKey: " + rowKey);
          	   return;
             }
             
             if(up.hasDgInfo()){
            	 UserProfile2.DemographicInfo dgInfo = up.getDgInfo();
	           //sex
          	   if(dgInfo.getSexCount() != 0){
          		   buffer.append(dgInfo.getSex(0).getValue());
          	   }
          	   else{
          		   buffer.append("0");
          	   }
          	   //ages
          	   if(dgInfo.getAgesCount() != 0){
          		   buffer.append("," + dgInfo.getAges(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //city
          	   if(dgInfo.getCityCount() != 0){
          		   buffer.append("," + dgInfo.getCity(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //married
          	   if(dgInfo.hasMarried()){
          		   if(dgInfo.getMarried()){
          			   buffer.append(",结婚");
          		   }
          		   else{
          		     buffer.append(",0");
          		   }
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
               //children
          	   if(dgInfo.hasHasBaby()){
          		   if(dgInfo.getHasBaby()){
          			   buffer.append(",有孩子");
          		   }
          		   else{
          			   buffer.append(",0");
          		   }
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //district
          	   if(dgInfo.getDistrictCount() != 0){
          		   buffer.append("," + dgInfo.getDistrict(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }   
             }
             else{
          	   buffer.append("0");
          	   for(int i = 0; i < 5; ++i){
          		   buffer.append(",0");
          	   }
             }
             //营销特征
             if(up.hasMarketFt()){
            	 UserProfile2.MarketingFeatures mkFt = up.getMarketFt();
          	   //price_level
          	   if(mkFt.getPriceLevelCount() != 0){
          		   buffer.append("," + mkFt.getPriceLevel(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //con_money
          	   if(mkFt.getConMoneyCount() != 0){
          		   buffer.append("," + mkFt.getConMoney(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //con_periods
          	   if(mkFt.hasConPeriods()){
          		 UserProfile2.CycInfo cycInfo = mkFt.getConPeriods();
          		   if(cycInfo.getCycInfoCount() != 0){
          			   buffer.append("," + cycInfo.getCycInfo(0).getValue());
          		   }
          		   else{
          			   buffer.append(",0");
          		   }
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
             }
             else{
          	   for(int i = 0; i < 3; ++i){
          		   buffer.append(",0");
          	   }
             }
             //上网特征
             if(up.hasInterFt()){
            	 UserProfile2.InternetFeatures interFt = up.getInterFt();
          	   //terminal_types
          	   if(interFt.getTerminalTypesCount() != 0){
          		   buffer.append("," + interFt.getTerminalTypes(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //terminal_brands
          	   if(interFt.getTerminalBrandsCount() != 0){
          		   buffer.append("," + interFt.getTerminalBrands(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
               //操作系统
          	   if(interFt.getOperSystemsCount() > 0){
          		   buffer.append("," + interFt.getOperSystems(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
           	   //browser
          	   if(interFt.getBrowserCount() > 0){
          		   buffer.append("," +interFt.getBrowser(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
          	   //online_time
          	   if(interFt.getOnlineTimeCount() != 0){
          		   buffer.append("," + interFt.getOnlineTime(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
               //internet_time
          	   if(interFt.getInternetTimeCount() != 0){
          		   buffer.append("," + interFt.getInternetTime(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
               //frequency
          	   if(interFt.getFrequencyCount() != 0){
          		   buffer.append("," + interFt.getFrequency(0).getValue());
          	   }
          	   else{
          		   buffer.append(",0");
          	   }
             }
             else{
          	   for(int i = 0; i < 7; ++i){
          		   buffer.append(",0");
          	   }
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
          		 if(index == up.getCidInfoCount()){
          		   buffer.append(",0");
          		 }
          		 else{
          		   Map<String, Integer> bussineCateMap = new TreeMap<String, Integer>();
          		   if(cidInfo.getIndusCount() != 0){
          			 UserProfile2.IndustryInfo indusInfo = null;
          			   for(int i = 0; i < cidInfo.getIndusCount(); ++i){
          				   indusInfo = cidInfo.getIndus(i);
          				   if(indusInfo.getFirstCateCount() == 0){
          					   continue;
          				   }
          				   for(int findex= 0; findex < indusInfo.getFirstCateCount(); ++findex){
          					 UserProfile2.FirstCategory firstCate = indusInfo.getFirstCate(findex);
          					   if(firstCate.getName().isEmpty() || firstCate.getName().equals("")){
          						   continue;
          					   }
          					   String firstCateTmp = firstCate.getName();        					   
          					   for(int sindex = 0; sindex < firstCate.getSecondCateCount(); ++sindex){
          						 UserProfile2.SecondCategory secondCate = firstCate.getSecondCate(sindex);
          						   if(secondCate.getName().isEmpty() || secondCate.getName().equals("")){
          							   continue;
          						   }
          						   String secondCateTmp = secondCate.getName();
          						   for(int tindex = 0; tindex < secondCate.getThirdCateCount(); ++tindex){
          							 UserProfile2.ThirdCategory thirdCate = secondCate.getThirdCate(tindex);
          							   if(thirdCate.getName().isEmpty() || thirdCate.getName().equals("")){
          								   continue;
          							   }
          							   String thirdCateTmp = thirdCate.getName();
          							   bussineCateMap.put(firstCateTmp+"$" + secondCateTmp + "$" + thirdCateTmp,  thirdCate.getWeight());
          						   }
          					   }
          				   }
          			   }
          		   }
          		   StringBuffer cateBuffer = new StringBuffer("");
          		   int currentNum = 1;
          		   for(Map.Entry<String, Integer> iter : entriesSortedByValues(bussineCateMap)){
          			  if(currentNum == 1){
          				  cateBuffer.append(iter.getKey() + ">" + iter.getValue());
          			  }
          			  else{
          				cateBuffer.append( "|" + iter.getKey() + ">" + iter.getValue());
          			  }
          			  if(++currentNum > 5){
          					break;
          			  }
          		   }	
          		   String cateStr = cateBuffer.toString();
          		   if(cateStr.equals("")){
          			   cateStr="0";
          		   }
          		   buffer.append("," + cateStr.replace("\n", ""));
          	   
          		 }
          		 
          	   //brand
          	   if(index == up.getCidInfoCount()){
          		   buffer.append(",0");
          	   }
          	   else{
          		   Map<String, Integer> brandMap = new TreeMap<String, Integer>();
          		   if(cidInfo.getIndusCount() != 0){
          			 UserProfile2.IndustryInfo indusInfo = null;
          			 for(int i = 0; i < cidInfo.getIndusCount(); ++i){
          				indusInfo = cidInfo.getIndus(i);
          				for(int j = 0; j < indusInfo.getBrandsCount(); ++j){
          					brandMap.put(indusInfo.getBrands(j).getValue(), indusInfo.getBrands(j).getWeight());
          				}
          			 }
          		   }
              	   StringBuffer brandBuffer = new StringBuffer();
              	   int currentNum = 1;
          		   for(Map.Entry<String, Integer> iter : entriesSortedByValues(brandMap)){
          			  if(currentNum == 1){
          				brandBuffer.append(iter.getKey() + ">" + iter.getValue());
          			  }
          			  else{
          				brandBuffer.append( "|" + iter.getKey() + ">" + iter.getValue());
          			  }
          			  if(++currentNum > 5){
          					break;
          			  }
          		   }
          		   String brandStr = brandBuffer.toString();
          		   if(brandStr.equals("")){
          			   brandStr="0";
          		   }
          		   buffer.append("," + brandStr.replace("\n", ""));
          	   }
        	   if(index == up.getCidInfoCount()){
        		   buffer.append(",0");
        	   }
        	   else{
        		   Map<String, Integer> MediaCateMap = new TreeMap<String, Integer>();
         		   if(cidInfo.getIndusCount() != 0){
         			  UserProfile2.IndustryInfo indusInfo = null;
        			   for(int i = 0; i < cidInfo.getIndusCount(); ++i){
        				   indusInfo = cidInfo.getIndus(i);
        				   if(indusInfo.getMediaCateCount() == 0){
        					   continue;
        				   }
        				   for(int findex= 0; findex < indusInfo.getMediaCateCount(); ++findex){
        					   UserProfile2.FirstCategory firstCate = indusInfo.getMediaCate(findex);
        					   if(firstCate.getName().isEmpty() || firstCate.getName().equals("")){
        						   continue;
        					   }
        					   String firstCateTmp = firstCate.getName();
        					   for(int sindex = 0; sindex < firstCate.getSecondCateCount(); ++sindex){
        						   UserProfile2.SecondCategory secondCate = firstCate.getSecondCate(sindex);
        						   if(secondCate.getName().isEmpty() || secondCate.getName().equals("")){
        							   continue;
        						   }
        						   String secondCateTmp = secondCate.getName();
        						   for(int tindex = 0; tindex < secondCate.getThirdCateCount(); ++tindex){
        							   UserProfile2.ThirdCategory thirdCate = secondCate.getThirdCate(tindex);
        							   if(thirdCate.getName().isEmpty() || thirdCate.getName().equals("")){
        								   continue;
        							   }
        							   String thirdCateTmp = thirdCate.getName();
        							   MediaCateMap.put(firstCateTmp+"$" + secondCateTmp + "$" + thirdCateTmp, thirdCate.getWeight());
        						   }
        					   }
        				   }
        			   }
        		   }
         		  StringBuffer mediaBuffer = new StringBuffer("");
              	  int currentNum = 1;
              	  for(Map.Entry<String, Integer> iter : entriesSortedByValues(MediaCateMap)){
              		  if(currentNum == 1){
              			mediaBuffer.append(iter.getKey() + ">" + iter.getValue());
              		  }
             		  else{
             			 mediaBuffer.append( "|" + iter.getKey() + ">" + iter.getValue());
             		  }
              		  if(++currentNum > 5){
              			  break;
              		  }
             	  }
              	  String mediaStr = mediaBuffer.toString();
              	  if(mediaStr.equals("")){
       			   mediaStr="0";
       			  }
              	  buffer.append("," + mediaStr.replace("\n", ""));
              	 }
        	  }
	         else{
            	  for(int i = 0; i < 3; ++i){
            		   buffer.append(",0");
            	   }
	           }
	           context.write(new Text(up.getUid()), new Text(buffer.toString()));
          }
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
    	  Configuration conf = HBaseConfiguration.create();
          //set parameters
          String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
          if(otherArgs.length != 4){
       	   System.out.println("The number of parameters is wrong.");
       	   return;
          }
          conf.set("mapred.textoutputformat.separator", ",");
          
          conf.set("Table", otherArgs[0]);
          conf.set("FAMILY", otherArgs[1]);
          //hbase configuration
          conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
          conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
          conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
          Job job = new Job(conf, "getHwUp");
          job.setJarByClass(getDetailUpFromScan.class);
          //DEBUG_B
          for(int i = 0; i < otherArgs.length; ++i){
        	  System.out.println("The " + i +" argments is " + otherArgs[i]);
          }
          DistributedCache.addCacheFile(new URI(otherArgs[2]), job.getConfiguration());
          Scan scan = new Scan();
          scan.addFamily(Bytes.toBytes(otherArgs[1]));
          scan.setCaching(1000);
          scan.setCacheBlocks(false);
          TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(otherArgs[0]), scan, MyMapper.class, Text.class, Text.class, job);
          job.setNumReduceTasks(0);
          FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); 
          System.exit(job.waitForCompletion(true) ? 0 : 1);
         
     }

}
