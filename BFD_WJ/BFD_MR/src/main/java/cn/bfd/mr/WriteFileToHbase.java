package cn.bfd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class WriteFileToHbase {
	
    
     public static class SplitMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable>{
  		public Map<String, Integer> cityLevelMap = new HashMap<String, Integer>();

		protected void setup(Context context)throws IOException, InterruptedException {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String finName = null;
			finName = localFiles[0].toString();
			BufferedReader attr_reader = new BufferedReader(new FileReader(finName));
			String line = "";
			while (((line = attr_reader.readLine()) != null)) {
				if (line.trim().isEmpty() == true) {
					continue;
				}
				String[] colVals = line.trim().split("\t");
				if (colVals.length !=2) {
					continue;
				}
				if(Integer.valueOf(colVals[1]) > 2){
					cityLevelMap.put(colVals[0], 3);
				}
				cityLevelMap.put(colVals[0], Integer.valueOf(colVals[1]));
			}
			super.setup(context);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] array = value.toString().split(",");
			if(array.length != 5){
				System.out.println("Error: the length of array is not equal to 2 in map. " + value.toString());
				return;
			}
			//����HBase�е�Rowkey
			ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(array[1]));
			Put put = new Put(Bytes.toBytes(array[1]));
			
			Map<String, String> valMap = new HashMap<String, String>();
			//add provider information
			valMap.put("provider", array[4]);
			//add city info to valList
			valMap.put("city", array[2] + "," + array[3]);
			//add level info to valList
			if(cityLevelMap.containsKey(array[3])){
				int level = cityLevelMap.get(array[3]);
				if(level > 2){
					level = 3;
				}
				valMap.put("level",String.valueOf(level));
			}
			else{
				valMap.put("level", String.valueOf(0));
			}
			
            byte[] family=Bytes.toBytes("up");
            for(Entry<String, String> entry : valMap.entrySet()){
            	put.add(family, Bytes.toBytes(entry.getKey()),  Bytes.toBytes(entry.getValue()));
            }
            
            //context.write(rowkey, put);
        
		}
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
 		Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
        
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		if (otherArgs.length != 3) {
 			System.out.println("Usage: <inputDir> <table> <cache>");
 			System.exit(-1);
 		}
 		
 		String inputDir = otherArgs[0];
 		String table = otherArgs[1];
 		String cache = otherArgs[2];
 		
		Job job = new Job(conf, "WriteFileToHbase");
		job.setJarByClass(WriteFileToHbase.class);
		job.setMapperClass(SplitMapper.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Writable.class);
		job.setNumReduceTasks(0);
		
        //DEBUG_B
        for(int i = 0; i < otherArgs.length; ++i){
      	  System.out.println("The " + i +" argments is " + otherArgs[i]);
        }
        //DEBUG_E
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(inputDir);
        FileStatus[] stats = fs.listStatus(dir);
		for (FileStatus stat : stats) {
			if (stat.getPath().getName().endsWith(".dat")){
				// DEBUG
				System.out.println("Source Path: " + stat.getPath().getName());
				FileInputFormat.addInputPath(job, stat.getPath());
			}
		}
		DistributedCache.addCacheFile(new URI(cache), job.getConfiguration());
        System.exit(job.waitForCompletion(true) ? 0 : 1);
     }

}
