package cn.bfd.data_bank;


import cn.bfd.protobuf.PortraitClass;
import cn.bfd.tools.*;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;


/**
 * version 20151218
 * author: yu.fu
 * email: yu.fu@baifendian.com
 * description: 统计用户画像各个维度的分布值
 */
public class GetAttrDistributionMapperTmp extends Mapper<LongWritable,Text,Text, IntWritable>{

	private static Logger LOG = Logger.getLogger(GetAttrDistributionMapperTmp.class);

	//配置Hbase选项
	private String hbase_thrift_ip = null; //hbase的thrift服务ip地址
	private int hbase_thrift_port ;//hbase的thrift服务端口
	private int scan_gid_size;//批处理从hbase中取数据条数
	private String hbase_table = null;//hbase表名
	private String family = null;//hbase列簇
	private List<String> columns = new ArrayList<String>();//保存hbase的列名
	private HbaseUtils hbaseUtils = null;  //处理Hbase的
	private List<String> gids= new ArrayList<String>();
	private List<String> missedrowkeys = new ArrayList<String>();
	private Text keyText = new Text();
	private Text valueText = new Text();

	private String filterAttributePath = null;

	//配置相关映射表的HDFS路径
	private String attr_map_path = null;
	private String age_map_path = null;
	private String sex_map_path = null;
	private String online_time_map_path = null;
	private String internet_time_map_path = null;
	private String frequence_map_path = null;
	private String terminial_type_path = null;
	private String cityLevel_map_path = null;
	private String dg_map_path = null;
	private FileSystem fs = null;




	//配置相关映射表结构
	private Map<String, String> attrMap = new HashMap<String, String>();
	private Map<String, String> internetTimeMap = new HashMap<String, String>();
	private Map<String, String> onlineTimeMap = new HashMap<String, String>();
	private Map<String, String> terminalTypeMap = new HashMap<String, String>();
	private Map<String, String> frequenceMap = new HashMap<String, String>();
	private Map<String, String> prefixTypeMap = new HashMap<String, String>();
	private Map<String, Map<String, String>> internetMaps = new HashMap<String, Map<String, String>>();

	private Map<String, String> dgMap = new HashMap<String, String>();
	private Map<String, String> cityLevelMap = new HashMap<String, String>();
	private Map<String, String> ageMap = new HashMap<String, String>();

	//通用常量
	private final static String INTNETPREFIX = "/上网特征";
	private final static String MARKETPREFIX = "/营销特征";
	private final static String DGINFOPREFIX = "/人口属性";

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	//设置查询条件
	private Map<String, String> filterAttribute = new HashMap<String, String>();

	/*
	 * 设置MR相关的参数
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		fs = FileSystem.get(conf);
		age_map_path = conf.get("age_map_path");
		sex_map_path = conf.get("sex_map_path");
		internet_time_map_path = conf.get("internet_time_map_path");
		online_time_map_path = conf.get("online_time_map_path");
		frequence_map_path = conf.get("frequence_map_path");
		terminial_type_path = conf.get("terminial_type_path");
		cityLevel_map_path = conf.get("cityLevel_map_path");
		dg_map_path = conf.get("dg_map_path");
		attr_map_path = conf.get("attr_map_path");

		//hbase table 配置
		family = conf.get("family");
		hbase_table = conf.get("hbase_table");
		hbase_thrift_ip = conf.get("hbase_thrift_ip");
		hbase_thrift_port = Integer.valueOf(conf.get("hbase_thrift_port"));

		//获取属性筛选条件映射表
		filterAttributePath = conf.get("filter_attribute_path");

		//初始化要获取的Hbase对应的列名，存入到对应的List中;
		for(String column : conf.get("column").trim().split(",")){
			columns.add(column);
		}

		//初始化对应的Hbase处理对象
		hbaseUtils = new HbaseUtils(hbase_table, hbase_thrift_ip, hbase_thrift_port);

		//初始化批处理的个数
		scan_gid_size = Integer.valueOf(conf.get("scan_gid_size"));


		//填充属性映射表-attrMap
		FileUtils.FillStrValMap(attrMap, attr_map_path, ",", fs);
		LOG.info("attrMap length: " + attrMap.size());

		//填充上网时段的映射表-internetTimeMap
		FileUtils.FillStrValMap(internetTimeMap, internet_time_map_path, ",", fs);
		LOG.info("internetTimeMap length: " + internetTimeMap.size());
		internetMaps.put("internet_time", internetTimeMap);

		//填充上网时长的映射表-onlineTimeMap
		FileUtils.FillStrValMap(onlineTimeMap, online_time_map_path, ",", fs);
		LOG.info("onlineTimeMap length: " + onlineTimeMap.size());
		internetMaps.put("online_time", onlineTimeMap);

		//填充上网频次的映射表-frequenceMap
		FileUtils.FillStrValMap(frequenceMap, frequence_map_path, ",", fs);
		LOG.info("frequenceMap length: " + frequenceMap.size());
		internetMaps.put("frequency", frequenceMap);

		//填充终端类型的映射表-terminalTypeMap
		FileUtils.FillStrValMap(terminalTypeMap, terminial_type_path, ",", fs);
		LOG.info("terminalTypeMap length: " + terminalTypeMap.size());
		internetMaps.put("terminal_types", terminalTypeMap);

		//填充年龄的映射表(生物和互联网)-ageMap
		FileUtils.FillStrValMap(ageMap, age_map_path, ",", fs);
		LOG.info("ageMap length: " + ageMap.size());

		//填充城市的映射表-cityLevelMap
		FileUtils.FillStrValMap(cityLevelMap, cityLevel_map_path, ",", fs);
		LOG.info("cityMap length: " + cityLevelMap.size());

		//填充人口属性映射表-dgMap
		FileUtils.FillStrValMap(dgMap, dg_map_path, ",", fs);
		LOG.info("cityMap length: " + cityLevelMap.size());

		//添加对应的字符串表示以及其对应的用户画像字段，两者之间的关系保存到prefixTypeMap中
		String[] allPrefix = {"/长期购物偏好", "/长期兴趣偏好", "/短期购物偏好", "/短期兴趣偏好", "/当下需求特征", "/潜在需求特征" };
		String[] allTypes = {"ec_indus", "media_indus", "timely_ec_indus", "timely_media_indus", "current_demand","potential_demand"};
		for(int i = 0; i < allPrefix.length; ++i){
			prefixTypeMap.put(allPrefix[i], allTypes[i]);
		}

		//填充查询条件映射表
		FileUtils.FillStrValMap(filterAttribute, filterAttributePath, ",", fs);




	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		if (gids.size() > 0) {
			LOG.info("cleanup: the size of gids: " + gids.size());
			getUserProfileFromHBase(gids, columns, context);
		}
	}


	/**
	 * 将从Hbase多列中获取的字节内容，转化为对应的Up对象，并且将其存入到一个Map中
	 * @param res 从Hbase中获取的一行结果（可能包含多列）
	 * @return 返回最终的Map对象结果
	 * @throws InvalidProtocolBufferException
	 */
	public Map<String, Object> fillUpMap(TResult res) throws InvalidProtocolBufferException{

		if(res == null){
			LOG.error("fillUpMap: res is null");
			return null;
		}

		Map<String, Object> objMap = new HashMap<String, Object>();

		for(TColumnValue resValue: res.getColumnValues()){
			String qualifier = new String(resValue.getQualifier());
//			if(qualifier.equals("gid")){
//				objMap.put("gid", new String(resValue.getValue()));
//			}
			if(qualifier.equals("update_time")){
				objMap.put("update_time", new String(resValue.getValue()));
			}
			else if(qualifier.equals("demographic")){
				try{
					PortraitClass.DemographicInfo dgInfo = PortraitClass.DemographicInfo.parseFrom(resValue.getValue());
					objMap.put("demographic", dgInfo);
				}catch(InvalidProtocolBufferException e){
					LOG.warn("fillUpMap: dgInfo is null");
					e.printStackTrace();
					continue;
				}
			}
			else if(qualifier.equals("inet_PC")){
				try{
					PortraitClass.InternetFeatures internet_ft = PortraitClass.InternetFeatures.parseFrom(resValue.getValue());
					objMap.put("inet_PC",internet_ft);
				}catch(InvalidProtocolBufferException e){
					LOG.warn("#fillUpMap: inet_PC is null");
					e.printStackTrace();
					continue;
				}
			}
			else if(qualifier.equals("inet_Mobile")){
				try{
					PortraitClass.InternetFeatures internet_ft = PortraitClass.InternetFeatures.parseFrom(resValue.getValue());
					objMap.put("inet_Mobile",internet_ft);
				}catch(InvalidProtocolBufferException e){
					e.printStackTrace();
					LOG.warn("fillUpMap: inet_Mobile is null");
					continue;
				}
			}
			else if(qualifier.equals("market")){
				try{
					PortraitClass.MarketingFeatures mk_ft = PortraitClass.MarketingFeatures.parseFrom(resValue.getValue());
					objMap.put("market",mk_ft);
				}catch(InvalidProtocolBufferException e){
					LOG.warn("fillUpMap: market is null");
					e.printStackTrace();
					continue;
				}
			}
			else if(qualifier.equals("cid_Cbaifendian")){
				try{
					PortraitClass.CidInfo cidInfo = PortraitClass.CidInfo.parseFrom(resValue.getValue());
					objMap.put("cid_Cbaifendian",cidInfo);
				}catch(InvalidProtocolBufferException e){
					LOG.warn("fillUpMap: cid_Cbaifendian is null");
					e.printStackTrace();
					continue;
				}
			}
		}

		if(!objMap.containsKey("gid")){
			//获取对应结果用户画像的gid,其中的rowkey为gid:gid
			if(res.getRow() == null){
				LOG.error("fillUpMap:The rowkey is null.");
				return null;
			}
			String rowkey = new String(res.getRow());
			objMap.put("gid", rowkey);
		}

		return objMap;
	}



	/**
	 *
	 *  根据TGet列表，获取对应的结果
	 */

	public void getUpAttributes(List<TGet> gets, List<String> gids, Context context) throws InterruptedException,IOException{

		int retryCount = 0;    //重试的次数
		Set<String> fieldSet = new HashSet<String>(); //记录查询失败的gid值


		//初始条件判断
		if(gets.size() == 0 || gets == null){
			return;
		}

		if(gets.size() != gids.size() || gids.size() == 0 || gids == null){
			LOG.warn("getDocs: The size of gets does not equals to the size of gids. ");
			return;
		}

		//重试机制，获取结果失败，重新请求5次
		while(retryCount < 5){
			try {
				//记录重试次数
				retryCount++;

				//批量获取指定列的返回结果
				List<TResult> results = hbaseUtils.getResults(gets);
				if(results == null){
					LOG.info("getDocs: results is null" );
					continue;
				}

				//遍历该结果获得对应的存储结果
				for(int i = 0; i < results.size(); ++i){
					Map<String, Integer> resultMap = new HashMap<String, Integer>();  //记录用户画像各个维度的统计值
					TResult res = results.get(i);
					//获取结果为空，放入到操作失败的failedList中
					if(res == null){
						fieldSet.add(gids.get(i));
						LOG.warn("getDocs: The gid is failed. " + gids.get(i));
						continue;
					}



					//从Hbase中获取用户画像存放在objMap中
					Map<String, Object> objMap = null;
					objMap = fillUpMap(res);

					if(objMap == null){
						LOG.info("objMap object is null");
						continue;
					}
					else{
						LOG.info("objMap object size: " + objMap.size());
					}

					if(objMap.containsKey("demographic") || objMap.containsKey("inet_PC") || objMap.containsKey("inet_Mobile") || objMap.containsKey("market") || objMap.containsKey("cid_Cbaifendian")){
						resultMap.put("Total",1);
					}


//					//DEBUG测试
//					for(Map.Entry<String, Object> entry : objMap.entrySet()){
//						if("gid".equals(entry.getKey())){
//							word.set("gid" + "," + UpUtils.reverseString((String)entry.getValue()));
//							context.write(word, one);
//						}
//					}


					if(objMap != null && objMap.size() != 0){
						//resultMap.put("Total", 1);

						//统计PC端的上网特征
						if(objMap.containsKey("inet_PC")){
							GetUserInternetAttribute.getPCInternetsDistribution(objMap.get("inet_PC"), INTNETPREFIX, internetMaps, resultMap);
						}
						else{
							LOG.info("objMap inet_PC is empty.");
						}

						//统计PC端的上网特征
						if(objMap.containsKey("inet_Mobile")){
							GetUserInternetAttribute.getMobileInternetsDistribution(objMap.get("inet_Mobile"), INTNETPREFIX, internetMaps, resultMap);
						}
						else{
							LOG.info("objMap inet_Mobile is empty.");
						}

						GetUserInternetAttribute.getFirstClass(objMap, INTNETPREFIX, resultMap);

						//统计人口属性
						if(objMap.containsKey("demographic")){
							PortraitClass.DemographicInfo dg_info = (PortraitClass.DemographicInfo)objMap.get("demographic");
							if(dg_info != null){
								String prefix = "people_data";
								GetDemographicInfoAttribute.getSexDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, resultMap);
								GetDemographicInfoAttribute.getCityDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, cityLevelMap, resultMap);
								GetDemographicInfoAttribute.getAgesDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, ageMap, resultMap);
								GetDemographicInfoAttribute.getBioGenderDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, resultMap);
								GetDemographicInfoAttribute.getBioAgeDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, ageMap, resultMap);
								GetDemographicInfoAttribute.getHasBabyDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, resultMap);
								GetDemographicInfoAttribute.getMarriedDemographicInfoDistribution(dg_info, DGINFOPREFIX, dgMap, resultMap);
								GetDemographicInfoAttribute.getFirstClassDistribution(objMap, DGINFOPREFIX, resultMap);
								GetDemographicInfoAttribute.getSecondClassDistribution(objMap, DGINFOPREFIX, resultMap);
							}
						}
						else{
							LOG.info("objMap demographic is empty.");
						}

						//统计标准品类
						if(objMap.containsKey("cid_Cbaifendian")){
							PortraitClass.CidInfo cidInfo = (PortraitClass.CidInfo)objMap.get("cid_Cbaifendian");
							if(cidInfo != null){
								for(Map.Entry<String, String> entry : prefixTypeMap.entrySet()){
									String new_prefix = entry.getKey() + "/品类";
									if(cidInfo.hasEcIndus() && entry.getValue().equals("ec_indus")){
										//长期购物偏好
										GetUserProfileAttribute.getFirstCateDistribution(cidInfo.getEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getSecondCateDistribution(cidInfo.getEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getThirdCateDistribution(cidInfo.getEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFourthCateDistribution(cidInfo.getEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFifthCateDistribution(cidInfo.getEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										//添加长期购物偏好的统计
										resultMap.put(entry.getKey(), 1);
										resultMap.put(entry.getKey() + "/品类", 1);
									}

									if(cidInfo.hasTimelyEcIndus() && entry.getValue().equals("timely_ec_indus")){
										//短期购物偏好
										GetUserProfileAttribute.getFirstCateDistribution(cidInfo.getTimelyEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getSecondCateDistribution(cidInfo.getTimelyEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getThirdCateDistribution(cidInfo.getTimelyEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFourthCateDistribution(cidInfo.getTimelyEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFifthCateDistribution(cidInfo.getTimelyEcIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										//添加短期购物偏好的统计
										resultMap.put(entry.getKey(), 1);
										resultMap.put(new_prefix, 1);
									}

									if(cidInfo.hasMediaIndus() && entry.getValue().equals("media_indus")){
										//长期内容偏好
										GetUserProfileAttribute.getFirstCateDistribution(cidInfo.getMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getSecondCateDistribution(cidInfo.getMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getThirdCateDistribution(cidInfo.getMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFourthCateDistribution(cidInfo.getMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFifthCateDistribution(cidInfo.getMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										//添加长期内容偏好的统计
										resultMap.put(entry.getKey(), 1);
										resultMap.put(new_prefix, 1);
									}



									if(cidInfo.hasTimelyMediaIndus() && entry.getValue().equals("timely_media_indus")){
										//短期内容偏好
										GetUserProfileAttribute.getFirstCateDistribution(cidInfo.getTimelyMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getSecondCateDistribution(cidInfo.getTimelyMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getThirdCateDistribution(cidInfo.getTimelyMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFourthCateDistribution(cidInfo.getTimelyMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFifthCateDistribution(cidInfo.getTimelyMediaIndus(), entry.getValue(), new_prefix, attrMap, resultMap);

										//添加短期内容偏好的统计
										resultMap.put(entry.getKey(), 1);
										resultMap.put(new_prefix, 1);
									}

									if(cidInfo.hasCurrentDemand() && entry.getValue().equals("current_demand")){
										//当下购物需求
										GetUserProfileAttribute.getFirstCateDistribution(cidInfo.getCurrentDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getSecondCateDistribution(cidInfo.getCurrentDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getThirdCateDistribution(cidInfo.getCurrentDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFourthCateDistribution(cidInfo.getCurrentDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFifthCateDistribution(cidInfo.getCurrentDemand(), entry.getValue(), new_prefix, attrMap, resultMap);

										//添加当下购物需求的统计
										resultMap.put(entry.getKey(), 1);
										resultMap.put(new_prefix, 1);
									}

									if(cidInfo.hasPotentialDemand() && entry.getValue().equals("potential_demand")){
										//潜在购物需求
										GetUserProfileAttribute.getFirstCateDistribution(cidInfo.getPotentialDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getSecondCateDistribution(cidInfo.getPotentialDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getThirdCateDistribution(cidInfo.getPotentialDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFourthCateDistribution(cidInfo.getPotentialDemand(), entry.getValue(), new_prefix, attrMap, resultMap);
										GetUserProfileAttribute.getFifthCateDistribution(cidInfo.getPotentialDemand(), entry.getValue(), new_prefix, attrMap, resultMap);

										//添加潜在购物需求
										resultMap.put(entry.getKey(), 1);
										resultMap.put(new_prefix, 1);
									}

								}
							}
						}
						else{
							LOG.info("objMap cid_Cbaifendian is empty.");
						}

						//统计营销特征
						if(objMap.containsKey("market")){
							PortraitClass.MarketingFeatures mf = (PortraitClass.MarketingFeatures)objMap.get("market");
							GetMarketingFeaturesAttribute.getConCapacityFeaturesDistribution(mf, MARKETPREFIX,resultMap);
							GetMarketingFeaturesAttribute.getConPeriodMarketingFeaturesDistribution(mf, MARKETPREFIX, resultMap);
							GetMarketingFeaturesAttribute.getPriceSensitivityFeaturesDistribution(mf, MARKETPREFIX, resultMap);
							GetMarketingFeaturesAttribute.getPriceLevelMarketingFeaturesDistribution(mf, MARKETPREFIX, resultMap);
							GetMarketingFeaturesAttribute.getFirstClass(mf, MARKETPREFIX, resultMap);
						}
						else{
							LOG.info("objMap market is empty");
						}

						for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
							word.set(entry.getKey());
							context.write(word, one);
						}

						for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
							if(filterAttribute.containsKey(entry.getKey())){
								word.set(gids.get(i) + "," + entry.getKey());
								context.write(word, one);
							}
						}


		} else {
						LOG.info("The gid: " + gids.get(i) + " objMap is zero.");
					}

				}


				break;
			} catch (Exception  e) {
				LOG.error("Get gids from HBase Faild!!! retry [" + retryCount + "], message : [" + e.getMessage() + "]");
				Thread.sleep(2000);
				e.printStackTrace();
			}
		}

	}



	/**
	 * 从Hbase中获取指定gid的用户画像，将其保存在Pair列表中，并且返回
	 * @param gids 指定需要返回用户画像的列表
	 * @param  columns 保存指定列名的List列表
	 * @param context 保存上下文的环境变量
	 * @return 返回保存用户画像结果的Pair列表对象
	 */
	public void getUserProfileFromHBase(List<String> gids,
										List<String> columns,
										Context context) throws InterruptedException, IOException{
		List<TGet> gets = null;

		gets = hbaseUtils.getPatchMuliColumnGet(gids, family, columns);
		if(gets != null){
			LOG.info("################## getUserProfileFromHBase gets size: " + gets.size());
		}
		else{
			LOG.info("################## getUserProfileFromHBase gets size: null" );
		}

		//根据Get列表，从HBase中获取对应的用户画像列表
		getUpAttributes(gets,gids, context);

		//清除gids List里面保存的gid
		gids.clear();
		//DEBUG
		LOG.info("################## After gids.clear(): gets size is " + gids.size());

	}




	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String gid = value.toString();
			gids.add(gid);
			//批量获取指定的gid和cid进行处理
			if (gids.size() == scan_gid_size) {

				LOG.info("Before: The size of gids is : " + gids.size());
				//对指定的gid和cid读取Hbase，建立相关的索引
				getUserProfileFromHBase(gids, columns, context);
				LOG.info("After: The size of gids is: " + gids.size());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

