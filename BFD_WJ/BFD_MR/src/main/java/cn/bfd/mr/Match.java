package cn.bfd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class Match {
	
    
     public static class MergeMapper extends Mapper<Object, Text, Text, Text>{
    	 
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] array = value.toString().split(",");
			if(array.length != 5){
				System.out.println("Error: the length of array is not equal to 5 in map. " + value.toString());
				return;
			}
			
			//context.write(new Text(array[1]), new Text(array[1] +"\t" + array[2] + "\t" + array[3]));
			context.write(new Text(array[0] + "\t" + array[2] + "\t" + array[3] + "\t" + array[4]), new Text(array[1]));
		}
     }
     
 	public static class MergeReducer extends Reducer<Text, Text, Text, Text>{
 		
 		public Map<String, Integer> phoneMap = new HashMap<String, Integer>();
 		
 		
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String finName = null;

			
			finName = localFiles[0].toString();
			BufferedReader attr_reader = new BufferedReader(new FileReader(finName));
			String line = "";
			while (((line = attr_reader.readLine()) != null)) {
				if (line.trim().isEmpty() == true) {
					continue;
				}
				String[] colVals = line.trim().split(",");
				if (colVals.length != 1) {
					continue;
				}
				phoneMap.put(colVals[0], 1);
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				if(phoneMap.containsKey(val.toString())){
					context.write(new Text(val.toString()), new Text(key.toString()));
				}
			}
		}
		
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
 		Configuration conf = new Configuration();
 		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 		if (otherArgs.length != 4) {
 			System.out.println("Usage: <input> <output> <reduceNum> <cache>");
 			System.exit(-1);
 		}
 		conf.set("mapred.textoutputformat.separator", ",");
 		String inputDir = otherArgs[0];
 		int reducerNum = Integer.valueOf(otherArgs[2]);
		Job job = new Job(conf, "ProcessData");
		job.setJarByClass(Match.class);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

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
		DistributedCache.addCacheFile(new URI(otherArgs[3]), job.getConfiguration());
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
         
     }

}
