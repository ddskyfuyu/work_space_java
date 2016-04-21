package cn.bfd.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TransformMatrix {

	public static class DataMapper extends Mapper<Object, Text, Text, Text> {
		public static List<Integer> indexArray = new ArrayList<Integer>();
		//public static int index = 0;

		//set keyNum variable
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration conf = context.getConfiguration();
			//keyNum = Integer.parseInt(conf.get("keyNum", "10"));
			String index = conf.get("index", "");
			for(String item : index.split(",")){
				indexArray.add(Integer.parseInt(item));
			}
		}

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String strIns = value.toString();
			//strIns = strIns.replaceAll("\"", "");
			//DEBUG_B
			//System.out.println("Source:" + strIns);
			//System.out.println("keyNum: " + keyNum);
			//System.out.println("index: " + index);
			//DEBUG_E
			String[] array = strIns.split(",");
			
			System.out.println("The length of array:　" + array.length);
			//System.out.println("The first number : " + array[0]);
			
			for(Integer index : indexArray){
				if(index > (array.length - 1)){
					return;
				}
			}
			
//			if(!array[0].matches("\\d+")){
//				System.out.println(array[0]  + "is not digist");
//				return;
//			}
			StringBuffer buffer = new StringBuffer();
			//String id = String.valueOf((Integer.parseInt(array[0]) % keyNum) + 1);
			buffer.append(array[0]);
			//String gid =0 array[1];
			for(Integer index : indexArray){
				buffer.append(":" + array[index]);
			}
			//String val = array[index];
			//DEBUG_B
			//System.out.println("Value: " + val);
			//DEBUG_E
			context.write(new Text(array[0]), new Text(buffer.toString()));
		}
	}

	// the reduce class
	public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
		public static Map<String, Integer> cateIndexMap = new HashMap<String ,Integer>();
		public static Map<String, Integer> mediaIndexMap = new HashMap<String, Integer>();
		public static Map<String, String> cidToMediaCateMap = new HashMap<String, String>();
		public static int cateNum = 0;
		public static int mediaNum = 0;
		
		
		

		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(localFiles.length < 2){
				System.out.println("The length of localFiles is less than 2");
				return;
			}
			String finName = null;
			finName = localFiles[0].toString();
			BufferedReader attr_reader = new BufferedReader(new FileReader(finName));
			String line = "";
			int count = 0;
			while (((line = attr_reader.readLine()) != null)) {
				if (line.trim().isEmpty() == true) {
					continue;
				}
				String record = line.trim();
				cateIndexMap.put(record, count++);
			}
			cateNum = count;
			
			finName = localFiles[1].toString();
			BufferedReader cidType_reader = new BufferedReader(new FileReader(finName));
			count = 0;
			while((line = cidType_reader.readLine())!= null){
				if(line.trim().isEmpty() == true){
					continue;
				}
				String[] elems = line.trim().split("\t");
				if(elems.length < 2){
					System.out.println("elems is less than 2. " + line.trim());
					continue;
				}
				cidToMediaCateMap.put(elems[0].toLowerCase(), elems[1]);
				if(mediaIndexMap.containsKey(elems[1])){
					continue;
				}
				else{
					mediaIndexMap.put(elems[1], count + cateNum);
					count++;
				}
			}
			/*
			for(Entry<String, String> entry : cidToMediaCateMap.entrySet()){
				System.out.println("cidToMediaCateMap " + entry.getKey() + ":" + entry.getValue());
			}
			*/
			mediaNum = count;
			
			//System.out.println("mediaNum: " + mediaNum);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
			for (Text val : values) {
				Map<Integer, Double> weightMap = new HashMap<Integer, Double>();
				Map<String, Double> cidWeightMap = new HashMap<String, Double>();
				
				String[] valArray = val.toString().split(":");
				if(valArray.length < 5){
					System.out.println("The length of val is less than 2. " + val.toString());
				 	continue;
				}
				String gid = valArray[0];
				String sex = valArray[1]; //性别
				String age = valArray[2]; //年龄
				String children = valArray[3]; //孩子
				String marrage = valArray[4]; //婚姻
				String line = valArray[5];
				String[] catesArray = line.trim().split("\\|");
			
				for(String cates : catesArray){
					String[] cateArray = cates.split("\\$"); 
					if(cateArray.length < 1){
						continue;
					}
					String cate = cateArray[0];
					if(cateIndexMap.get(cate) == null){
						System.out.println(cate + " can not find in cateIndexMap");
						continue;
					}
					String[] weightArray = cateArray[cateArray.length - 1].split(">");
					if(weightArray.length < 1){
						System.out.println(cateArray[cateArray.length - 1] + "split by >, the size is less than 2 .");
						continue;
					}
					double weight = Double.parseDouble(weightArray[1]);
					if(!weightMap.containsKey(cateIndexMap.get(cate))){
						weightMap.put(cateIndexMap.get(cate), 0.0);
					}
					//System.out.println("Weight :" + weight);
					//System.out.println("cate: " + cate + ", weight: " + weightMap.get(cateIndexMap.get(cate)));
					weight += weightMap.get(cateIndexMap.get(cate));
					weightMap.put(cateIndexMap.get(cate), weight);
				}
				line = valArray[6];
				String[] cidArray = line.trim().split("\\|");
				for(String cid : cidArray){
					if(cidToMediaCateMap.get(cid.toLowerCase()) == null){
						System.out.println(cid.toLowerCase() + " can not find in cidToMediaCateMap");
						continue;
					}
					if(cidWeightMap.get(cidToMediaCateMap.get(cid.toLowerCase())) == null){
						cidWeightMap.put(cidToMediaCateMap.get(cid.toLowerCase()), 0.0);
					}
					double weight = 1.0;
					weight += cidWeightMap.get(cidToMediaCateMap.get(cid.toLowerCase()));
					cidWeightMap.put(cidToMediaCateMap.get(cid.toLowerCase()), weight);
				}
	
				int[] indexArray = new int[cateNum + mediaNum];
				for(int i = 0; i < indexArray.length; ++i){
					indexArray[i] = 0;
				}
				for(Entry<Integer, Double> entry : weightMap.entrySet()){
					indexArray[entry.getKey()] += entry.getValue();
				}
				
				for(Entry<String, Double> entry : cidWeightMap.entrySet()){
					indexArray[mediaIndexMap.get(entry.getKey())] += entry.getValue();
				}
				StringBuffer buffer = new StringBuffer();
			    //sex
				if(sex.isEmpty() || sex.equals("")){
					buffer.append("");
				}
				else{
					buffer.append(sex);
				}
//				else{
//					if(sex.equals("男")){
//						buffer.append("\t" + "1");
//					}
//					else if(sex.equals("女")){
//						buffer.append("\t" + "0");
//					}
//					else{
//						buffer.append("\t" + "0");
//					}
//				}
				//age
				if(age.isEmpty() || age.equals("")){
					buffer.append(",");
				}
				else{
					buffer.append("," + age);
				}
				//children
				if(children.isEmpty() || children.equals("")){
					buffer.append(",");
				}
				else{
					buffer.append(","+children);
				}
//				else{
//					if(marrage.equals("有小孩")){
//						buffer.append(",1");
//					}
//					else{
//						buffer.append("\t" + "0");
//					}
//				}
				//marriage
				if(marrage.isEmpty() || marrage.equals("")){
					buffer.append(",");
				}
				else{
					buffer.append(","+marrage);
				}
//				else{
//					if(marrage.equals("已婚")){
//						buffer.append("\t" + "1");
//					}
//					else{
//						buffer.append("\t" + "0");
//					}
//				}
				if(indexArray.length < 0){
					System.out.println("The length of indexArray is less than 0. ");
					continue;
				}
				buffer.append("," + String.valueOf(indexArray[0]));
				for(int i = 1; i < indexArray.length; ++i){
					buffer.append("," + String.valueOf(indexArray[i]));
				}
				context.write(new Text(gid), new Text(buffer.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// 閰嶇疆 hadoop job
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 6) {
			System.out.println("The length of arguments is " + otherArgs.length);
			System.out.println("Usage: <l-date>");
			System.exit(-1);
		}
		
		conf.set("mapred.textoutputformat.separator", ",");
		
		//set input path
		String inputPath = otherArgs[0];
		String outputPath = otherArgs[1];
		String mapPath = otherArgs[2];
		String cidPath = otherArgs[3];
		
		//set the number of keys
		//conf.set("keyNum", otherArgs[4]);
		System.out.println("Index: " + otherArgs[4]);
		conf.set("index", otherArgs[4]);
		
		//set the number of reduce
		int reduceNum = Integer.parseInt(otherArgs[5]);

		Job job = new Job(conf, "TransFormMatrix");
		job.setJarByClass(TransformMatrix.class);
		job.setMapperClass(DataMapper.class);
		job.setReducerClass(MatrixReducer.class);
		//set the type of output for mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//set the type of output for reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));		
		job.setNumReduceTasks(reduceNum);
		//transform files to hdfs
		DistributedCache.addCacheFile(URI.create(mapPath), job.getConfiguration());
		DistributedCache.addCacheFile(URI.create(cidPath), job.getConfiguration());
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
