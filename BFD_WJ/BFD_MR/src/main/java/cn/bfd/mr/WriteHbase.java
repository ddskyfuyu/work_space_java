package cn.bfd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class WriteHbase {
	
	public static class TextArrayWritable extends ArrayWritable {
		public TextArrayWritable() {
			super(Text.class);
		}

		public TextArrayWritable(String[] strings) {
			super(Text.class);
			Text[] texts = new Text[strings.length];
			for (int i = 0; i < strings.length; i++) {
				texts[i] = new Text(strings[i]);
			}
			set(texts);
		}
	}
	
    
     public static class SplitMapper extends Mapper<Object, Text, Text, TextArrayWritable>{
  		public Map<String, Integer> cityLevelMap = new HashMap<String, Integer>();

		protected void setup(Context context)throws IOException, InterruptedException {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String finName = null;

			// 鍒涘缓浼氬憳璧勬枡灞炴�瀛楀�?锛�
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

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] array = value.toString().split(",");
			if(array.length != 5){
				System.out.println("Error: the length of array is not equal to 2 in map. " + value.toString());
				return;
			}
			List<String> valList = new ArrayList<String>();
			
			int[] index = {4};
			//add provider info to valList
			for(int iter : index){
				valList.add(array[iter]);
			}
			//add city info to valList
			valList.add(array[2] + "," + array[3]);
			//add level info to valList
			if(cityLevelMap.containsKey(array[3])){
				int level = cityLevelMap.get(array[3]);
				if(level > 2){
					level = 3;
				}
				valList.add(String.valueOf(level));
			}
			else{
				valList.add(String.valueOf(0));
			}
			TextArrayWritable textArrayWritable = new TextArrayWritable((String[]) valList.toArray(new String[valList.size()]));
			context.write(new Text(array[1]), textArrayWritable);
		}
     }
     
 	public static class WriteReducer extends Reducer<Text, TextArrayWritable, Text, Text>{
 		

 		public static HTable outTable = null;
 		public static String family;
 		
 		
		protected void setup(Context context) throws IOException, InterruptedException {

			
			Configuration conf = context.getConfiguration();
			String table = conf.get("table", "-1");
			family = conf.get("family", "-1");
			if(table.equals("-1") || family.equals("-1")){
			 System.out.println("The params is wrong. ");
			 return;
			}
 			conf = HBaseConfiguration.create();
	        conf.set("hbase.zookeeper.quorum", "192.168.48.12,192.168.48.13,192.168.48.14");
	        conf.set("zookeeper.znode.parent", "/dp/bfdhbase");
	        conf.set("hbase.rootdir", "hdfs://192.168.48.29:8020/hbase");
	        //configure hbase
			try {
				outTable = new HTable(conf, Bytes.toBytes(table));
				outTable.setAutoFlush(true);
				outTable.setWriteBufferSize(0);

			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
			for (TextArrayWritable val : values) {
				String[] valArray = val.toStrings();
				String[] colNameList = {"provider","city","level"};
				Put put = new Put(Bytes.toBytes(key.toString()));
				for(int i = 0; i < colNameList.length; ++i){
					if((i == (colNameList.length - 1)) && (valArray[i].equals("0"))){
						continue; 
					}
					put.add(Bytes.toBytes(family),Bytes.toBytes(colNameList[i]), Bytes.toBytes(valArray[i]));
				}
				outTable.put(put);
			}
		}
		
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		if (otherArgs.length != 6) {
 			System.out.println("Usage: <l-date>");
 			System.exit(-1);
 		}
 		conf.set("mapred.textoutputformat.separator", ",");
 		conf.set("table", otherArgs[0]);
 		conf.set("family", otherArgs[1]);
 		String inputDir = otherArgs[2];
 		
		Job job = new Job(conf, "WriteHbase");
		job.setJarByClass(WriteHbase.class);
		job.setMapperClass(SplitMapper.class);
		job.setReducerClass(WriteReducer.class);
		//map key-value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		//reduce key-value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		int reducerNum = Integer.valueOf(otherArgs[5]);
		job.setNumReduceTasks(reducerNum);
		
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
		DistributedCache.addCacheFile(new URI(otherArgs[4]), job.getConfiguration());
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
     }

}
