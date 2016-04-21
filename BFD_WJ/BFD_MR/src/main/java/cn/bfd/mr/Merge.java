package cn.bfd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class Merge {
	
    
     public static class MergeMapper extends Mapper<Object, Text, Text, Text>{
    	 
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] array = value.toString().split(",");
			if(array.length < 2){
				System.out.println("Error: the length of array is not equal to 2 in map. " + value.toString());
				return;
			}
			context.write(new Text(array[0]), new Text(array[1]));
		}
     }
 	public static class MergeReducer extends Reducer<Text, Text, Text, Text>{
 		
 		public Map<String, String> phoneMd5Map = new HashMap<String, String>();
 		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] indexArray = {"age", "sex", "marriage", "children"};
			StringBuffer buffer = new StringBuffer();
			Map<String, String> dataMap = new HashMap<String, String>();
			for (Text val : values) {
				String[] array = val.toString().split(":");
				if(array.length != 2){
					System.out.println("Error: the length of array is not equal to 2 in reduce. " + val.toString());
					continue;
				}
				dataMap.put(array[0], array[1]);
			}
			if(dataMap.containsKey(indexArray[0])){
				buffer.append(dataMap.get(indexArray[0]));
			}
			else{
				buffer.append("");
			}
			for(int i = 1; i < indexArray.length; ++i){
				if(dataMap.containsKey(indexArray[i])){
					buffer.append("," + dataMap.get(indexArray[i]));
				}
				else{
					buffer.append(", ");
				}
			}
			context.write(key, new Text(buffer.toString()));
		}
		
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		if (otherArgs.length != 3) {
 			System.out.println("Usage: <l-date>");
 			System.exit(-1);
 		}
 		conf.set("mapred.textoutputformat.separator", ",");
 		String inputDir = otherArgs[0];
 		int reducerNum = Integer.valueOf(otherArgs[2]);
		Job job = new Job(conf, "Merge");
		job.setJarByClass(Merge.class);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);
		//配置map key-value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//配置reduce key-value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
         
     }

}
