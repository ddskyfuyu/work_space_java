package cn.bfd.es;

import cn.bfd.indexAttribute.*;
import cn.bfd.indexUp.IndexUpCategoryDimension;
import cn.bfd.indexUp.IndexUpDgDimension;
import cn.bfd.indexUp.IndexUpInternetDimension;
import cn.bfd.indexUp.IndexUpMarketDimension;
import cn.bfd.protobuf.PortraitClass;
import cn.bfd.utils.HbaseUtils;
import cn.bfd.utils.Pair;
import cn.bfd.utils.UpUtils;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.*;


/**
 * version 20151218
 * @author yu.fu
 *
 */

public class ESMapper extends Mapper<LongWritable,Text,Text, Text>{



	private static Logger LOG = Logger.getLogger(ESMapper.class);
	private String hbase_thrift_ip = null;//hbase的thrift服务ip地址
	private int hbase_thrift_port ;//hbase的thrift服务端口
	private int scan_gid_size;//批处理从hbase中取数据条数
	private String hbase_table = null;//hbase表名
	private String family = null;//hbase列簇
	private String column = null;//hbase与列簇对应的列
	private HbaseUtils hbaseUtils = null;

	private Client client = null; //es客户端
	private String cluster_name = null;//es集群名称
	private String es_ip = null;//es的IP
	private int es_port ;//es的端口
	private String index = null;//es索引名称
	private String type = null;//es与索引名称相对应的索引类型
	

	private List<String> gids= new ArrayList<String>();
	private List<String> missedrowkeys = new ArrayList<String>();
	private Text keyText = new Text();
	private Text valueText = new Text();


    private ParseProtobuf2JSON parseProtobuf2JSON = null;
    private String age_map_path = null;
    private String sex_map_path = null;
    private String internet_time_map_path = null;
    private String cid_fin = null;
    private FileSystem fs = null;
	private List<String> columns = new ArrayList<String>();    //保存对应的列名
    
    //
    
	/*
	 * 设置MR相关的参数
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		fs = FileSystem.get(conf);
		age_map_path = conf.get("age_map_path");
		sex_map_path = conf.get("sex_map_path");
		internet_time_map_path = conf.get("internet_time_map_path");
		cid_fin = conf.get("cid_fin");

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

		//初始化ES配置
		cluster_name = conf.get("cluster_name");
		index = conf.get("es_index");
		es_ip = conf.get("es_ip");
		es_port = Integer.valueOf(conf.get("es_port"));
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build();
		client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(es_ip, es_port));

		//初始化批处理的个数
		scan_gid_size = Integer.valueOf(conf.get("scan_gid_size"));


		//初始化转化JSON的方法
	    parseProtobuf2JSON = getParseProtobuf2JSON();
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		if (gids.size() > 0) {
			indexHBase(gids,context);
		}
		if (client !=null) {
			client.close();
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
			if(qualifier.equals("gid")){
				objMap.put("gid", new String(resValue.getValue()));
			}
			else if(qualifier.equals("update_time")){
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
			String[] items = UpUtils.reverseString(rowkey).split(":");
			if (items.length != 2){
				LOG.error("fillUpMap: The rowkey is wrong. " + rowkey);
				return null;
			}
			objMap.put("gid", items[1]);
		}

		return objMap;
	}



	
	/**
	 *
	 *  根据TGet列表，获取对应的结果
	 */

	public ArrayList<Pair> getDocs(List<TGet> gets, List<String> gids, boolean columnIsEmpty, Context context) throws InterruptedException{

		ArrayList<Pair> docs = new ArrayList<Pair>();
		int retryCount = 0;
		List<String> findgids = new ArrayList<String>();
		if(gets.size() == 0){
			return docs;
	    }

		//重试机制，重试次数为5
        while(retryCount<5){
			try {
				//记录重试次数
				retryCount++;

				//批量获取指定列的返回结果
				List<TResult> results = hbaseUtils.getResults(gets);
				if(results == null){
					continue;
				}

				//遍历该结果获得对应的存储结果
				for (int i = 0; i < results.size(); ++i) {
					String rowKey = null;
					PortraitClass.Portrait up = null;
					//从Hbase中获取用户画像存放在up对象中
					if(!results.get(i).getColumnValues().isEmpty()){
						rowKey = new String(results.get(i).getRow());
						try{
							up = PortraitClass.Portrait.parseFrom(results.get(i).getColumnValues().get(0).getValue());
						}catch(Error error){
							findgids.add(UpUtils.reverseString(rowKey));
							keyText.set(error.getMessage());
							valueText.set(rowKey);
							context.write(keyText, valueText);
							continue;
						}catch(Exception e){
							findgids.add(UpUtils.reverseString(rowKey));
							keyText.set(e.getMessage());
							valueText.set(rowKey);
							context.write(keyText, valueText);
							continue;
						}
					}

					if (up == null) {
						continue;
					}
					try{
						//根据用户画像生成对应的JSON对象
						JSONObject doc = new JSONObject();
						parseProtobuf2JSON.parse2JSon(up, doc);

						String gid = up.getUuid();
						docs.add(new Pair(gid, doc.toString()));
					}catch(Error error){
						findgids.add(UpUtils.reverseString(rowKey));
						keyText.set(error.getMessage());
						valueText.set(rowKey);
						context.write(keyText, valueText);
						continue;
					}catch(Exception e){
						findgids.add(UpUtils.reverseString(rowKey));
						keyText.set(e.getMessage());
						valueText.set(rowKey);
						context.write(keyText, valueText);
						continue;
					}
					findgids.add(UpUtils.reverseString(rowKey));
				}

				int totalNumber = gets.size();
				int docsHitNumber = docs.size();
				int gidNumber = gids.size();
				int findHitNumber = findgids.size();
				int hbaseHitNumber = results.size();
				int missHitNumber = 0;

				//去除已执行查询操作的gid
				gids.removeAll(findgids);

				//记录下column名称为空的rowKey,在第二次查询中如果还未找到则输出
				if (!columnIsEmpty) {
					missedrowkeys.clear();
					missedrowkeys.addAll(gids);
					missHitNumber = missedrowkeys.size();
					LOG.info("First Get From Hbase [Total: " + totalNumber + "gid---------" + gidNumber + ",docs Hit:" + docsHitNumber + ", find:" + findHitNumber + ", Miss:" + missHitNumber + ",hbase result:" + hbaseHitNumber + "]");
				}else{
					missHitNumber = gids.size();
					LOG.info("Second Get From Hbase [Total: " + totalNumber + "gid---------" + gidNumber + ",docs Hit:" + docsHitNumber + ", find:" + findHitNumber + ", Miss:" + missHitNumber + ",hbase result:" + hbaseHitNumber + "]");
					for (String missedRowkey : gids) {
						keyText.set("missedkey");
						valueText.set(missedRowkey);
						context.write(keyText, valueText);
					}
				}
				break;
			} catch (Exception  e) {
				//transport.close();
				LOG.error("getDocs: Get gids from HBase Faild!!! retry [" + retryCount + "] message [" + e.getMessage() +"]");
				Thread.sleep(2000);
				e.printStackTrace();
			}
		}
		return docs;
	}

	/**
	 *
	 *  根据TGet列表，获取对应的结果
	 */

	public ArrayList<Pair> getDocs(List<TGet> gets, List<String> gids, Context context) throws InterruptedException{

		ArrayList<Pair> docs = new ArrayList<Pair>();
		int retryCount = 0;    //重试的次数
		Set<String> fieldSet = new HashSet<String>();

		//初始条件判断
		if(gets.size() == 0 || gets == null){
			return docs;
		}
		if(gets.size() != gids.size() || gids.size() == 0 || gids == null){
			LOG.warn("getDocs: The size of gets does not equals to the size of gids. ");
			return docs;
		}

		//重试机制，获取结果失败，重新请求5次
		while(retryCount<5){
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
					if(objMap != null && objMap.size() != 0){
						JSONObject doc = new JSONObject();
						parseProtobuf2JSON.parse2JSon(objMap, doc);
						docs.add(new Pair(gids.get(i), doc.toString()));
					}
				}

				int totalNumber = gets.size();
				int docsHitNumber = docs.size();
				int gidNumber = gids.size();
				int failedNumber = fieldSet.size();
				int findHitNumber = totalNumber - failedNumber ;
				int hbaseHitNumber = results.size();

				LOG.info("First Get From Hbase [Total: " + gidNumber + ",Docs:" + docsHitNumber + ", Finish:" + findHitNumber + ", failed:" + failedNumber + ",hbase result:" + hbaseHitNumber +"]" );
				break;
			} catch (Exception  e) {
				LOG.error("Get gids from HBase Faild!!! retry [" + retryCount + "], message : ["+ e.getMessage() +"]");
				Thread.sleep(2000);
				e.printStackTrace();
			}
		}
		return docs;
	}



	/**
	 * 从Hbase中获取指定gid的用户画像，将其保存在Pair列表中，并且返回
	 * @param gids 指定需要返回用户画像的列表
	 * @param  columns 保存指定列名的List列表
	 * @param context 保存上下文的环境变量
	 * @return 返回保存用户画像结果的Pair列表对象
	 * @throws JSONException
	 * @throws TIOError
	 * @throws TException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	 ArrayList<Pair> getUserProfileFromHBase(List<String> gids,
											 List<String> columns,
											 Context context) throws JSONException, TIOError, TException, InterruptedException, IOException {
		 List<TGet> gets = null;

		 gets = hbaseUtils.getPatchMuliColumnGet(gids, family, columns);
		 if(gets != null){
			 LOG.info("################## getUserProfileFromHBase gets size: " + gets.size());
		 }
		 else{
			 LOG.info("################## getUserProfileFromHBase gets size: null" );
		 }

		 //System.out.println();
		 //根据Get列表，从HBase中获取对应的用户画像列表
		 ArrayList<Pair> docs = getDocs(gets,gids,context);
		 return docs;
	    }

	ArrayList<Pair> getUserProfileFromHBase(List<String> gids,
												 boolean columnIsEmpty,
												 Context context) throws JSONException, TIOError, TException, InterruptedException, IOException {
		List<TGet> gets = null;
		gets = hbaseUtils.getGets(gids, family, column);
		System.out.println();
		//根据Get列表，从HBase中获取对应的用户画像列表
		ArrayList<Pair> docs = getDocs(gets,gids,columnIsEmpty,context);
		return docs;
	}



	/**
	 * 
	 * 在es中建立索引
	 */
	public void indexDocs(ArrayList<Pair> docs){
		if(type == null){
			return;
		}

		if (!docs.isEmpty()) {
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
	  		for(int i=0; i<docs.size(); ++i) {
	  			bulkRequestBuilder.add(client.prepareIndex(index, type, docs.get(i).first).setSource(docs.get(i).second));
	  		}
	  		
	  		long start = System.currentTimeMillis();
	  		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
	  		if(bulkResponse.hasFailures()) {
	  			LOG.error("indexDocs: faild messge," + bulkResponse.buildFailureMessage());
	  		}

			LOG.info("[Upload docs number: " + docs.size() + "] [spent: " + (System.currentTimeMillis() - start) + "]");
		}

	}

	public void indexDocs(ArrayList<Pair> docs, String cid){
		if (!docs.isEmpty()) {
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for(int i=0; i<docs.size(); ++i) {
				bulkRequestBuilder.add(client.prepareIndex(index, cid, docs.get(i).first).setSource(docs.get(i).second));
			}

			long start = System.currentTimeMillis();
			BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
			if(bulkResponse.hasFailures()) {
				System.out.println(" faild messge : " + bulkResponse.buildFailureMessage() + "for " + cid + " type");
			}

			System.out.println("[Upload docs number: " + docs.size() + "] [spent: " + (System.currentTimeMillis() - start) + " for " + cid + "]");
		}

	}

	/**
	 *
	 * 针对gid数据先在hbase表中查找记录，再在es中建立索引，
	 * 如果column列名非空，针对在hbase中未匹配的数据要在空列名下再查找一次
	 *
	 * @param gids, 字符串列表, 存放所有gid
	 * @param context, 上下文类型，存放相关的配置信息
	 * @return void
	 */
	public void indexHBase(List<String> gids, Context context) {
		try {
			//根据gids列表，从Hbase中获取相关的列表信息
			indexUpFromHbaseBasedCid(gids, context);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 针对gid和cid数据先在hbase表中查找记录，再在es中建立索引，针对不同的cid分别建立相关的索引，并且对未匹配的gid重新
	 * 查询一遍，建立相关的索引
	 *
	 * @param cgList 每个元素是是由cid\001gid组成
	 * @param context 上下文类型，存放相关的配置信息
	 *
	 */
	public void indexUpFromHbaseBasedCid(List<String> cgList, Context context) {
		//构建一个Map结构体，它将cid作为key，将对应的gid列表作为value
		Map<String, List<String>> cgMap = new HashMap<String, List<String>>();
		for(String cg : cgList){
			String[] items = cg.trim().split(",");
			if (items.length != 2){
				System.out.println("The cg is wrong. " + cg);
				continue;
			}
			if(!cgMap.containsKey(items[0])){
				cgMap.put(items[0], new ArrayList<String>());
			}
			cgMap.get(items[0]).add(items[1]);
		}



		//依据cid，从Hbase获取相关的用户画像，以cid作为type, 更新相关type的索引
		try {
			for(Map.Entry<String, List<String>> entry : cgMap.entrySet()){
				List<String> gids = entry.getValue();
				ArrayList<Pair> docs  = getUserProfileFromHBase(gids,columns, context);
				indexDocs(docs, entry.getKey());
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			cgList.clear();
		}
	}
	
	public ParseProtobuf2JSON getParseProtobuf2JSON(){

		  IndexBooleanLevelAttribute indexBooleanLevelAttribute = new IndexBooleanLevelAttribute();
          IndexStringLevelAttribute indexStringLevelAttribute = new IndexStringLevelAttribute();
          IndexIntLevelAttribute indexIntLevelAttribute = new IndexIntLevelAttribute();
          IndexDoubleLevelAttribute indexDoubleLevelAttribute = new IndexDoubleLevelAttribute();
          IndexTimeStampLevelAttribute indexTimeStampLevelAttribute = new IndexTimeStampLevelAttribute();
          IndexFloatLevelAttribute indexFloatLevelAttribute = new IndexFloatLevelAttribute();
          IndexCategoryArrayObject indexCategoryArrayObject = new IndexCategoryArrayObject();
          IndexLongLevelAttribute indexLongLevelAttribute = new IndexLongLevelAttribute();



          IndexMethodCategoryNestedObject indexMethodCategoryNestedObject = new IndexMethodCategoryNestedObject(indexFloatLevelAttribute,
                                                                                                                   indexStringLevelAttribute,
                                                                                                                   indexIntLevelAttribute,
                                                                                                                   indexTimeStampLevelAttribute);

          IndexFirstCategoryNestedObject indexFirstCategoryNestedObject = new IndexFirstCategoryNestedObject(indexFloatLevelAttribute,
                                                                                                             indexStringLevelAttribute,
                                                                                                             indexIntLevelAttribute,
                                                                                                             indexTimeStampLevelAttribute);

          IndexSecondCategoryNestedObject indexSecondCategoryNestedObject = new IndexSecondCategoryNestedObject(indexFloatLevelAttribute,
                                                                                                                indexStringLevelAttribute,
                                                                                                                indexIntLevelAttribute,
                                                                                                                indexTimeStampLevelAttribute);

          IndexThirdCategoryNestedObject indexThirdCategoryNestedObject = new IndexThirdCategoryNestedObject(indexFloatLevelAttribute,
                                                                                                             indexStringLevelAttribute,
                                                                                                             indexIntLevelAttribute,
                                                                                                             indexTimeStampLevelAttribute);

          IndexFourthCategoryNestedObject indexFourthCategoryNestedObject = new IndexFourthCategoryNestedObject(indexFloatLevelAttribute,
                                                                                                                indexStringLevelAttribute,
                                                                                                                indexIntLevelAttribute,
                                                                                                                indexTimeStampLevelAttribute);

          IndexFifthCategoryNestedObject indexFifthCategoryNestedObject = new IndexFifthCategoryNestedObject(indexFloatLevelAttribute,
                                                                                                             indexStringLevelAttribute,
                                                                                                             indexIntLevelAttribute,
                                                                                                             indexTimeStampLevelAttribute);


          //初始化IndexUpDgDimension对象
          IndexUpDgDimension upDgDimension = new IndexUpDgDimension(indexBooleanLevelAttribute,
                                                                    indexStringLevelAttribute,
                                                                    indexIntLevelAttribute,
                                                                    age_map_path,
                                                                    sex_map_path,
				                                                    fs);

          //初始化IndexUpCategoryDimension对象
          IndexUpCategoryDimension upCategoryDimension = new IndexUpCategoryDimension(indexCategoryArrayObject,
                                                                                      indexIntLevelAttribute,
                                                                                      indexDoubleLevelAttribute,
                                                                                      indexStringLevelAttribute,
                                                                                      indexMethodCategoryNestedObject,
                                                                                      indexFirstCategoryNestedObject,
                                                                                      indexSecondCategoryNestedObject,
                                                                                      indexThirdCategoryNestedObject,
                                                                                      indexFourthCategoryNestedObject,
                                                                                      indexFifthCategoryNestedObject);
          //初始化IndexUpInternetDimension
          IndexUpInternetDimension indexUpInternetDimension = new IndexUpInternetDimension(indexCategoryArrayObject,
                                                                                           indexIntLevelAttribute,
                                                                                           indexStringLevelAttribute,
                                                                                           internet_time_map_path,fs);
//          //初始化IndexUpOldInternetDimension
//          IndexUpOldInternetDimension indexUpOldInternetDimension = new IndexUpOldInternetDimension(indexCategoryArrayObject,
//                                                                                                    indexIntLevelAttribute,
//                                                                                                    indexStringLevelAttribute);

          //IndexUpMarketDimension indexUpMarketDimension = new IndexUpMarketDimension(indexIntLevelAttribute);

		//初始化IndexUpMarketDimension
		IndexUpMarketDimension indexUpMarketDimension = new IndexUpMarketDimension(indexIntLevelAttribute,
				                                                                   indexStringLevelAttribute,
				                                                                    indexDoubleLevelAttribute,
				                                                                    indexMethodCategoryNestedObject,
				                                                                    indexCategoryArrayObject);
          ParseProtobuf2JSON parseProtobuf2JSON = new ParseProtobuf2JSON(cid_fin,
				                                                         upDgDimension,
				                                                         upCategoryDimension,
                                                                         indexUpInternetDimension,
                                                                         indexUpMarketDimension,
				                                                         fs);


		parseProtobuf2JSON.setIndexStringAttribute(indexStringLevelAttribute);
		parseProtobuf2JSON.setIndexLongAttribute(indexLongLevelAttribute);
		parseProtobuf2JSON.setIndexArrayObject(indexCategoryArrayObject);
        
        return parseProtobuf2JSON;

	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String gid = value.toString();
			gids.add(gid);
			//批量获取指定的gid和cid进行处理
			if (gids.size() == scan_gid_size) {
				//对指定的gid和cid读取Hbase，建立相关的索引
				indexHBase(gids,context);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
