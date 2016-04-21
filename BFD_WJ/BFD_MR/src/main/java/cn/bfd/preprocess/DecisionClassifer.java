package cn.bfd.preprocess;

import cn.bfd.tools.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class DecisionClassifer {

	private static Logger LOG = Logger.getLogger(DecisionClassifer.class);
	private final static String[] categorysNames = {"firstCategory", "secondCategory","threeCategory","fourCategory","fiveCategory"};


	public static class MergeMapper extends Mapper<Object, Text, Text, Text>{

		private Map<String, String> age_classifier = new HashMap<String, String>();
		private Map<String, String> gender_classifier = new HashMap<String, String>();
		private double ageRatio = 0.5;
		private double genderRatio = 0.5;

		protected void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);

			//获取性别的规则配置文件
			String genderPath = conf.get("gender.configuration.path", "");
			if("".equals(genderPath)){
				LOG.error(genderPath + " file is not exists.");
				System.exit(-1);
			}
			FileUtils.FillStrValMap(gender_classifier, genderPath, ",", fs);

			//DEBUG
			for(Map.Entry<String, String> entry : gender_classifier.entrySet()){
				LOG.info("gender_classifier: " + entry.getKey() + "," + entry.getValue());
			}

            //获取年龄的规则配置文件
			String agePath = conf.get("age.configuration.path", "");
			if("".equals(agePath)){
				LOG.error(agePath + " file is not exists.");
				System.exit(-1);
			}
			FileUtils.FillStrValMap(age_classifier, agePath, ",", fs);

			//DEBUG
			for(Map.Entry<String, String> entry : age_classifier.entrySet()){
				LOG.info("age_classifier: " + entry.getKey() + "," + entry.getValue());
			}

			//年龄阈值比例（max/total)
			ageRatio = Double.valueOf(conf.get("age.configuration.ratio", "0.5"));
			LOG.info("age ratio: " + ageRatio);

			//性别阈值比例（max/total)
			genderRatio = Double.valueOf(conf.get("gender.configuration.ratio","0.5"));
			LOG.info("gender ratio: " + genderRatio);

		}
    	 
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//定义存储年龄和性别结果的map结构体
			Map<String, Integer> genderMap = new HashMap<String, Integer>();
			Map<String, Integer> ageMap = new HashMap<String, Integer>();

			String[] array = value.toString().trim().split("\t");

			String final_val = " ";

			if(array.length != 5){
				LOG.warn("Error: the length of array is not equal to 5 in map. " + value.toString());
				return;
			}

			//电商品类划分
			String[] business_cates = array[1].trim().split(",");
			for(String cate : business_cates){
				String[] levelCates = cate.trim().split("\\$");
				for(int i = 0; i < levelCates.length; ++i){
					String business_key = "business:" + categorysNames[i] + ":" + levelCates[i];
					//匹配gender规则
					if(gender_classifier.containsKey(business_key)){
						String gender_val = gender_classifier.get(business_key);
						String[] vals = gender_val.trim().split(">");
						if(vals.length != 2){
							continue;
						}
						if(!genderMap.containsKey(vals[0])){
							genderMap.put(vals[0],0);
						}
						//LOG.info(business_key + ":" + "gender:" + Integer.valueOf(vals[1]));
						genderMap.put(vals[0], genderMap.get(vals[0]) + Integer.valueOf(vals[1]));
					}
					//匹配age规则
					if(age_classifier.containsKey(business_key)){
						String age_val = age_classifier.get(business_key);
						String[] vals = age_val.trim().split(">");
						if(vals.length != 2){
							continue;
						}
						if(!ageMap.containsKey(vals[0])){
							ageMap.put(vals[0],0);
						}
						//LOG.info(business_key + ":" + "age:" + Integer.valueOf(vals[1]));
						ageMap.put(vals[0],ageMap.get(vals[0]) + Integer.valueOf(vals[1]));
					}
				}
			}

			//品牌划分
			String[] brands = array[2].trim().split(",");
			for(String brand : brands){
				String brand_key = "brand:" + brand;
				//匹配gender规则
				if(gender_classifier.containsKey(brand_key)){
					String gender_val = gender_classifier.get(brand_key);
					String[] vals = gender_val.trim().split(">");
					if(vals.length != 2){
						continue;
					}
					if(!genderMap.containsKey(vals[0])){
						genderMap.put(vals[0],0);
					}
					//LOG.info(brand_key + ":" + "gender:" + Integer.valueOf(vals[1]));
					genderMap.put(vals[0],genderMap.get(vals[0]) + Integer.valueOf(vals[1]));
				}
				//匹配age规则
				if(age_classifier.containsKey(brand_key)){
					String age_val = age_classifier.get(brand_key);
					String[] vals = age_val.trim().split(">");
					if(vals.length != 2){
						continue;
					}
					if(!ageMap.containsKey(vals[0])){
						ageMap.put(vals[0],0);
					}
					//LOG.info(brand_key + ":" + "age:" + Integer.valueOf(vals[1]));
					ageMap.put(vals[0],ageMap.get(vals[0]) + Integer.valueOf(vals[1]));
				}
			}

			//媒体品类划分
			String[] media_cates = array[3].trim().split(",");
			for(String cate : media_cates){
				String[] levelCates = cate.trim().split("\\$");
				for(int i = 0; i < levelCates.length; ++i){
					String media_key = "meida:" + categorysNames[i] + ":" + levelCates[i];
					//匹配gender规则
					if(gender_classifier.containsKey(media_key)){
						String gender_val = gender_classifier.get(media_key);
						String[] vals = gender_val.trim().split(">");
						if(vals.length != 2){
							continue;
						}
						if(!genderMap.containsKey(vals[0])){
							genderMap.put(vals[0],0);
						}
						//LOG.info(media_key + ":" + "gender:" + Integer.valueOf(vals[1]));
						genderMap.put(vals[0],genderMap.get(vals[0]) + Integer.valueOf(vals[1]));
					}
					//匹配age规则
					if(age_classifier.containsKey(media_key)){
						String age_val = age_classifier.get(media_key);
						String[] vals = age_val.trim().split(">");
						if(vals.length != 2){
							continue;
						}
						if(!ageMap.containsKey(vals[0])){
							ageMap.put(vals[0],0);
						}
						//LOG.info(media_key + ":" + "age:" + Integer.valueOf(vals[1]));
						ageMap.put(vals[0],ageMap.get(vals[0]) + Integer.valueOf(vals[1]));
					}
				}
			}

			//进行判别
			int age_sum =0, gender_sum = 0, age_max = 0, gender_max = 0;
			String age_val = null;
			String gender_val = null;
			for(Map.Entry<String, Integer> entry : genderMap.entrySet()){
				if(gender_max < entry.getValue()){
					gender_max = entry.getValue();
					gender_val = entry.getKey();
				}
				gender_sum += entry.getValue();
			}

			LOG.info("gender_sum: " + gender_sum +",gender_max:" + gender_max);
			if(gender_val == null){
				LOG.info("gender_val: null");
			}
			else{
				LOG.info("gender_val: " + gender_val);
			}
			if(gender_val != null && gender_max >= gender_sum * genderRatio){
				final_val = gender_val;
			}
			LOG.info("##########1: " + final_val);
			for(Map.Entry<String, Integer> entry : ageMap.entrySet()){
				if(gender_max < entry.getValue()){
					age_max = entry.getValue();
					age_val = entry.getKey();
				}
				age_sum += entry.getValue();
			}
			LOG.info("age_sum: " + age_sum +",age_max:" + age_max);
			if(age_val == null){
				LOG.info("age_val: null");
			}
			else{
				LOG.info("age_val: " + age_val);
			}

			if(age_val != null && age_max >= age_sum * ageRatio){
				final_val = final_val + "\t" + age_val;
			}
			else{
				final_val = final_val + "\t" + " ";
			}
			LOG.info("##########2: " + final_val);
			context.write(value, new Text(final_val));
		}

     }
     
// 	public static class MergeReducer extends Reducer<Text, IntWritable, Text, Text>{
//
//
//
//		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//			int sum = 0;
//			for (IntWritable val : values) {
//				sum += val.get();
//			}
//
//			if(sum != 0){
//				context.write(key, new Text(String.valueOf(sum)));
//			}
//		}
//
//     }
    
    
     		
    
    
     public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 if (otherArgs.length != 6) {
			 System.out.println("Usage: <input> <output> <gender-config> <age-config> <gender-ratio> <age-ratio>" );
			 System.exit(-1);
		 }

		 //设置变量
		 String inputFile = otherArgs[0];
		 String outputDir = otherArgs[1];
		 String genderFileName = otherArgs[2];
		 String ageFileName = otherArgs[3];
		 String genderRatio = otherArgs[4];
		 String ageRatio = otherArgs[5];

		 //配置MR的相关参数
		 conf.set("mapred.textoutputformat.separator", "\t");
		 //配置路径文件
		 conf.set("gender.configuration.path", genderFileName);
		 conf.set("age.configuration.path", ageFileName);
		 conf.set("age.configuration.ratio", ageRatio);
		 conf.set("gender.configuration.ratio", genderRatio);

		 Job job = Job.getInstance(conf, "DecisionClassifer");
		 job.setJarByClass(DecisionClassifer.class);
		 job.setMapperClass(MergeMapper.class);
		 //job.setReducerClass(MergeReducer.class);

		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 job.setNumReduceTasks(0);
		 FileInputFormat.addInputPath(job, new Path(inputFile));
		 FileOutputFormat.setOutputPath(job, new Path(outputDir));
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
         
     }

}
