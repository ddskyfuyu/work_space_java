package cn.bfd.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;


public class ProcessData {
	private final static IntWritable one = new IntWritable(1);
	private static Logger LOG = Logger.getLogger(ProcessData.class);
	private final static String[] categorysNames = {"firstCategory", "secondCategory","threeCategory","fourCategory","fiveCategory"};


	public static class MergeMapper extends Mapper<Object, Text, Text, IntWritable>{


    	 
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] array = value.toString().trim().split("\t");
			if(array.length != 5){
				LOG.warn("Error: the length of array is not equal to 5 in map. " + value.toString());
				return;
			}

			//电商品类划分
			String[] business_cates = array[1].trim().split(",");
			for(String cate : business_cates){
				String[] levelCates = cate.trim().split("\\$");
				for(int i = 0; i < levelCates.length; ++i){
					context.write(new Text("business:" + categorysNames[i] + ":" + levelCates[i]), one);
				}
			}

			//品牌划分
			String[] brands = array[2].trim().split(",");
			for(String brand : brands){
				context.write(new Text("brand:" + brand), one);
			}

			//媒体品类划分
			String[] media_cates = array[3].trim().split(",");
			for(String cate : media_cates){
				String[] levelCates = cate.trim().split("\\$");
				for(int i = 0; i < levelCates.length; ++i){
					context.write(new Text("media:" + categorysNames[i] + ":" + levelCates[i]), one);
				}
			}

			//消费等级划分
			context.write(new Text("price_level:" + array[4]), one);
		}
     }
     
 	public static class MergeReducer extends Reducer<Text, IntWritable, Text, Text>{
 		


		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			if(sum != 0){
				context.write(key, new Text(String.valueOf(sum)));
			}
		}
		
     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 if (otherArgs.length != 3) {
			 System.out.println("Usage: <input> <output> <reduceNum>");
			 System.exit(-1);
		 }

		 //设置变量
		 String inputFile = otherArgs[0];
		 String outputDir = otherArgs[1];
		 int reducerNum = Integer.valueOf(otherArgs[2]);


		 //配置MR的相关参数
		 conf.set("mapred.textoutputformat.separator", ",");

		 Job job = Job.getInstance(conf, "ProcessData");
		 job.setJarByClass(ProcessData.class);
		 job.setMapperClass(MergeMapper.class);
		 job.setReducerClass(MergeReducer.class);

		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(IntWritable.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 job.setNumReduceTasks(reducerNum);
		 FileInputFormat.addInputPath(job, new Path(inputFile));
		 FileOutputFormat.setOutputPath(job, new Path(outputDir));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
         
     }

}
