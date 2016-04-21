package cn.bfd.data_bank;

/**
 * 本程序主要用来针对用户画像的各个指标维度进行统计
 * 使用MR程序，统计各个指标的分布，输出到指定的HDFS目录下
 * author: yu.fu
 * date: 2015-12-25
 * phone: 15120071966
 */
import cn.bfd.tools.FileUtils;
import cn.bfd.tools.HbaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class GetGidLengthFromHbase extends Configured implements Tool{

	public static class GetGidLengthMapper extends Mapper<Object, Text, Text, Text> {

		//设置Map对应的输出类型
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private static Logger LOG = Logger.getLogger(GetGidLengthMapper.class);



		private List<String> gids= new ArrayList<String>();
		private int scan_gid_size = 0;
		private String family = null;
		private String hbase_table = null;
		private String hbase_thrift_ip = null;
		private int hbase_thrift_port;
		private List<String> columns = new ArrayList<String>();
		private HbaseUtils hbaseUtils = null;
		private int cut_length = 0;


		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();

			//hbase table 配置
			family = conf.get("family");
			hbase_table = conf.get("hbase_table");
			hbase_thrift_ip = conf.get("hbase_thrift_ip");
			hbase_thrift_port = Integer.valueOf(conf.get("hbase_thrift_port"));

			//初始化要获取的Hbase对应的列名，存入到对应的List中;
			for(String column : conf.get("column").trim().split(",")){
				columns.add(column);
			}

			//初始化对应的Hbase处理对象
			hbaseUtils = new HbaseUtils(hbase_table, hbase_thrift_ip, hbase_thrift_port);

			//初始化批处理的个数
			scan_gid_size = Integer.valueOf(conf.get("scan_gid_size"));

			//截断gid的长度设置
			cut_length = Integer.valueOf(conf.get("cut_length"));
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String gid = value.toString();
				gids.add(gid);
				//批量获取指定的gid和cid进行处理
				if (gids.size() == scan_gid_size) {
					LOG.info("Before: The size of gids is : " + gids.size());
					//对指定的gid和cid读取Hbase，建立相关的索引
					getLengthFromHbase(gids, columns, context);
					LOG.info("After: The size of gids is: " + gids.size());
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * 批量获取对应用户画像列表的各个用户画像在各个维度的长度，并且将结果输出到对应的HDFS中
		 *
		 * @param gids 保存批量获取用户画像的gid
		 * @param colums 保存要获取用户画像各个维度对应的列名
		 * @param context 保存MR对应的上下文关系，利用该变量将结果写入到指定的HDFS路径中
		 */

		private void getLengthFromHbase(List<String> gids, List<String> colums, Context context){
			try{

				List<TGet> gets = null;
				gets = hbaseUtils.getPatchMuliColumnGet(gids, family, columns);

				//初始条件判断-gets为null进行判断
				if(gets == null){
					LOG.error("The gets is null.");
					gids.clear();
					return;
				}

				//初始条件判断-gids为null进行判断
				if(gids == null){
					LOG.error("The gids is null.");
					gids.clear();
					return;
				}

				//初始条件判断-gids size为0进行判断
				if(gids.size() == 0){
					LOG.error("The size of gids is zero. ");
					return;
				}

				//初始条件判断-gids与gets的size关系
				if(gets.size() != gids.size()){
					LOG.error("The size of gets does not equals to the size of gids. ");
					gids.clear();
					return;
				}


				//批量获取指定列的返回结果
				List<TResult> results = hbaseUtils.getResults(gets);
				if(results == null){
					for(String gid : gids){
						if(gid != null){
							LOG.error("The result of gid is null. gid: " + gid);
						}
						else{
							LOG.error("The result of gid is null. gid: null");
						}
					}
				}
				else{
					//遍历该结果获得对应的存储结果
					for(int i = 0; i < results.size(); ++i){

						TResult res = results.get(i);
						//获取结果为空，将对应的gid存放在LOG日志中
						if(res == null){
							LOG.error("The TResult is null. gid:" + gids.get(i));
							continue;
						}
						//从Hbase中获取用户画像各个维度的长度存放在lengthMap中
						Map<String, String> lengthMap = null;
						Map<String, Integer> keyTolengthMap = new HashMap<String, Integer>();
						lengthMap = getUpLength(res);

						if(lengthMap == null){
							LOG.error("lengthMap object is null");
							continue;
						}
						else{
							LOG.info("lengthMap object size: " + lengthMap.size());
						}

						//计算用户画像各个维度的长度
						int upLength = 0;

						//保存各个维度的长度
						List<String> dimWithLength = new ArrayList<String>();
						for(String colum : colums){
							if(lengthMap.containsKey(colum)){
								if("update_time".equals(colum)){
									dimWithLength.add("up." + colum + ":" + lengthMap.get(colum));
									continue;
								}
								upLength += Integer.valueOf(lengthMap.get(colum));
								dimWithLength.add("up." + colum + ":" + lengthMap.get(colum));
								keyTolengthMap.put("up." + colum, Integer.valueOf(lengthMap.get(colum)));
							}
							else{
								dimWithLength.add("up." + colum + ":0");
								keyTolengthMap.put("up." + colum, 0);
							}

						}

						//将总长度添加到对应的dimWithLength中
						dimWithLength.add("up.length:" + upLength);
						keyTolengthMap.put("up.length", upLength);
						if(isValidGid(keyTolengthMap, cut_length)){
							outputKey.set(gids.get(i));
							outputValue.set(StringUtils.join(dimWithLength, ","));
							context.write(outputKey, outputValue);
						}

					}

				}
			}catch(Exception e){
				LOG.error(e.getMessage());
			}finally {
				gids.clear();
			}
		}


		private boolean isValidGid(Map<String, Integer> map, int total_length){
			if(!map.containsKey("up.length")){
				return false;
			}
			int length = map.get("up.length");
			if(length >= total_length){
				return true;
			}
			else{
				return false;
			}
		}


		/**
		 * 将从Hbase多列中获取对应的字节长度，并且将其存入到一个LinkedeMap中
		 *
		 * @param res 从Hbase中获取的一行结果（可能包含多列）
		 * @return 返回最终的Map对象结果
		 *
		 */
		public Map<String, String> getUpLength(TResult res){



			if(res == null){
				LOG.error("fillUpMap: res is null");
				return null;
			}

			Map<String, String> lengthMap = new HashMap<String, String>();

			for(TColumnValue resValue: res.getColumnValues()){
				String qualifier = new String(resValue.getQualifier());

				//计算用户画像人口属性的长度
				if(qualifier.equals("demographic")){
					lengthMap.put("demographic", String.valueOf(resValue.getValue().length));
				}
				//计算用户画像PC上网特征的长度
				else if(qualifier.equals("inet_PC")){
					lengthMap.put("inet_PC", String.valueOf(resValue.getValue().length));
				}
				//计算用户画像Mobile上网特征的长度
				else if(qualifier.equals("inet_Mobile")){
					lengthMap.put("inet_Mobile", String.valueOf(resValue.getValue().length));
				}
				//计算用户画像营销特征的长度
				else if(qualifier.equals("market")){
					lengthMap.put("market", String.valueOf(resValue.getValue().length));
				}
				//计算用户画像标准品类的长度
				else if(qualifier.equals("cid_Cbaifendian")){
					lengthMap.put("cid_Cbaifendian", String.valueOf(resValue.getValue().length));
				}
				else if(qualifier.equals("update_time")){
					lengthMap.put("update_time", new String(resValue.getValue()));
				}
			}
			return lengthMap;
		}
	}


	
	public static void main(String[] args) throws Exception {
		if (args.length < 8) {
			System.out.println("<Usage> config_file hbase_table family column scan_size cut_length inputPath outputPath");
			System.exit(-1);
		}
		int result = ToolRunner.run(new GetGidLengthFromHbase(), args);
		System.exit(result);
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		
		FileSystem fs = FileSystem.get(configuration); 

        String config_file = args[0];
        String hbase_table = args[1];
        String family = args[2];
        String columns = args[3];
		String scan_gid_size = args[4];
		String length = args[5];
        String inputPath = args[6];
        String outputPath = args[7];



		Map<String,String> map = new HashMap<String,String>();
		FileUtils.FillStrValMap(map, config_file, "=", fs);

		//配置输出文件中列与列之间的分隔符
		configuration.set("mapred.textoutputformat.separator", ",");


	    // 配置hbase thrift ip
		if(map.containsKey("hbase_thrift_ip")){
			configuration.set("hbase_thrift_ip", map.get("hbase_thrift_ip"));
		}
		else{
			System.out.println("The hbase_thrift_ip does not exist in map. ");
			System.exit(-1);
		}

		// 配置hbase thrift port
		if(map.containsKey("hbase_thrift_port")){
			configuration.set("hbase_thrift_port", map.get("hbase_thrift_port"));
		}
		else{
			System.out.println("The hbase_thrift_port does not exist in map. ");
			System.exit(-1);
		}

		// 配置hbase表名
		if(hbase_table != null){
			configuration.set("hbase_table",hbase_table);
		}
		else{
			System.out.println("The table is null. ");
			System.exit(-1);
		}

		// 配置hbase列名字符串
		if(columns != null){
			configuration.set("column",columns);
		}
		else{
			System.out.println("The columns is null. ");
			System.exit(-1);
		}

		// 配置hbase的列簇名称
		if(family != null){
			configuration.set("family",family);
		}
		else{
			System.out.println("The family is null. ");
			System.exit(-1);
		}

		// 配置批处理的gid的个数
		if((scan_gid_size != null) && (Integer.valueOf(scan_gid_size) > 0)){
			configuration.set("scan_gid_size",scan_gid_size);
		}
		else{
			System.out.println("The scan_gid_size is wrong. ");
			System.exit(-1);
		}

		// 配置gid长度删除阈值
		if((length != null) && (Integer.valueOf(length) > 0)){
			configuration.set("cut_length",length);
		}
		else{
			System.out.println("The cut_length is wrong. ");
			System.exit(-1);
		}

		
        Job job = Job.getInstance(configuration, "GetGidLengthFromHbase");
        job.setJarByClass(GetGidLengthFromHbase.class);

		job.setMapperClass(GetGidLengthMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		if (args.length>8) {
			String min_split=args[8];
			//System.out.println("MinSplitSize before set:"+FileInputFormat.getMinSplitSize(job));
			FileInputFormat.setMinInputSplitSize(job, Long.parseLong(min_split));
			//System.out.println("MinSplitSize after set:"+FileInputFormat.getMinSplitSize(job));
		}
		if (args.length>9) {
			String max_split=args[9];
			//System.out.println("MaxSplitSize before set:"+FileInputFormat.getMaxSplitSize(job));
			FileInputFormat.setMaxInputSplitSize(job, Long.parseLong(max_split));
			//System.out.println("MaxSplitSize after set:"+FileInputFormat.getMaxSplitSize(job));
		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
