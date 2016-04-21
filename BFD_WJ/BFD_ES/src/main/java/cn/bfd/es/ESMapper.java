package cn.bfd.es;

import cn.bfd.indexAttribue.*;
import cn.bfd.indexUp.*;
import cn.bfd.protobuf.UserProfile2;
import cn.bfd.utils.Pair;
import cn.bfd.utils.UpUtils;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.thrift2.generated.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * version 20150817
 * @author tao.yu
 *
 */

public class ESMapper extends Mapper<LongWritable,Text,Text, Text>{

	private Client client = null; //es客户端
	private TTransport transport = null;//thrift服务
	private THBaseService.Iface hbase_client = null;//hbase客户端
	
	private String hbase_thrift_ip = null;//hbase的thrift服务ip地址
	private int hbase_thrift_port ;//hbase的thrift服务端口
	private int scan_gid_size;//批处理从hbase中取数据条数
	private String hbase_table = null;//hbase表名
	private String family = null;//hbase列簇
	private String column = null;//hbase与列簇对应的列

	private String cluster_name = null;//es集群名称
	private String es_ip = null;//es的IP
	private int es_port ;//es的端口
	private String index = null;//es索引名称
	private String type = null;//es与索引名称相对应的索引类型
	
	//private ParseProtobuf2 parseProtobuf = new ParseProtobuf2();
	private Map<String, String> mapConfig = new HashMap<String,String>();//处理Protobuf需要的额外信息
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
		
		family = conf.get("family");
		column = conf.get("column");
		scan_gid_size = Integer.valueOf(conf.get("scan_gid_size"));
		
		cluster_name = conf.get("cluster_name");
		index = conf.get("es_index");
		type = conf.get("es_type");
		es_ip = conf.get("es_ip");
		es_port = Integer.valueOf(conf.get("es_port"));
		
		hbase_table = conf.get("hbase_table");
		hbase_thrift_ip = conf.get("hbase_thrift_ip");
		hbase_thrift_port = Integer.valueOf(conf.get("hbase_thrift_port"));
		
		//mapConfig.put("CITY_LEVEL", conf.get("CITY_LEVEL"));
		//mapConfig.put("OPER_SYSTEMS", conf.get("OPER_SYSTEMS"));
		//mapConfig.put("BROWSER", conf.get("BROWSER"));

		//使用Thrift建立Hbase client
		transport = new TSocket(hbase_thrift_ip, hbase_thrift_port);
	    TProtocol protocol = new TBinaryProtocol(transport);
	    hbase_client = new THBaseService.Client(protocol);

		//DEBUG
		System.out.println("hbasetable--------"+hbase_table+"-----family-------"+family+"------column-----"+column);

		//
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build();
	    client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(es_ip,es_port));
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
	 *
	 * 根据gids, 从Hbase中批量获取列名不为空的用户画像记录
	 *
	 * @param gids 字符串列表 存放gid的字符串列表
	 * @return void
	 *
	 */
	public List<TGet> getGets(List<String> gids){
		String rowkey = null;
		List<TGet> gets = new ArrayList<TGet>();

		for (String gid : gids) {
			if (gid == null || gid.isEmpty()){
				continue;
			}
			//利用Thrift接口，批量产生Get，存放到对应的列表中
			TGet g = new TGet();
			//Hbase中存储的rowkey是gid的逆序
			rowkey = UpUtils.reverseString(gid);
			g.setRow(rowkey.getBytes());
			TColumn t = new TColumn();
			t.setFamily(family.getBytes());
			//判断列名是否为空
			if (column != null && column.trim().length() > 0) {
				t.setQualifier(column.getBytes());
			}else{
				t.setQualifier(Bytes.toBytes(""));
			}
			g.addToColumns(t);
			gets.add(g);
		 }
		return gets;
	}
	
	/**
	 * 
	 * 取得hbase表中column名称是空的记录
	 *
	 * @param gids 字符串列表 存放的是gid列表
	 * @return 返回thrift TGet的批结果
	 *
	 */
	public List<TGet> getColumnIsEmptyGets(List<String> gids){

		List<TGet> gets = new ArrayList<TGet>();
		for (String gid: gids){
			if (gid == null || gid.isEmpty()){
				continue;
			}
			TGet g = new TGet();
			String rowkey = UpUtils.reverseString(gid);
			g.setRow(rowkey.getBytes());
			TColumn t = new TColumn();
			t.setFamily(family.getBytes());
			//列名为空
			t.setQualifier(Bytes.toBytes(""));
			g.addToColumns(t);
			gets.add(g);
		}
		return gets;
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

				//thrift接口批量获取TResult
				transport.open();
				ByteBuffer table = ByteBuffer.wrap(hbase_table.getBytes());
				List<TResult> results = hbase_client.getMultiple(table, gets);
				transport.close();
	            	
				for (int i = 0; i < results.size(); ++i) {
					String rowKey = null;
					UserProfile2.UserProfile up = null;
					//从Hbase中获取用户画像存放在up对象中
					if(!results.get(i).getColumnValues().isEmpty()){
						rowKey = new String(results.get(i).getRow());
						try{
							up = UserProfile2.UserProfile.parseFrom(results.get(i).getColumnValues().get(0).getValue());
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

						String gid = up.getUid();
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
					System.out.println("First Get From Hbase [Total: " + totalNumber + "gid---------"+gidNumber+",docs Hit:" + docsHitNumber +", find:" + findHitNumber+", Miss:" + missHitNumber+",hbase result:" + hbaseHitNumber +"]" );
				}else{
					missHitNumber = gids.size();
					System.out.println("Second Get From Hbase [Total: " + totalNumber +"gid---------"+gidNumber+ ",docs Hit:" + docsHitNumber +", find:" + findHitNumber+", Miss:" + missHitNumber+",hbase result:" + hbaseHitNumber +"]" );
					for (String missedRowkey : gids) {
						keyText.set("missedkey");
						valueText.set(missedRowkey);
						context.write(keyText, valueText);
					}
				}
				break;
			} catch (Exception  e) {
				transport.close();
				System.out.println("Get gids from HBase Faild!!! retry : " +retryCount+ "message : "+ e.getMessage());
				Thread.sleep(2000);
				e.printStackTrace();
			}
		}
		return docs;
	}
	
	 ArrayList<Pair> getUserProfileFromHBase(List<String> gids,
											 boolean columnIsEmpty,
											 Context context) throws JSONException, TIOError, TException, InterruptedException, IOException {
		 List<TGet> gets = null;
		 System.out.println("***************"+new Date().getTime()+"***********");
		 if (columnIsEmpty) {
			 //获取列名为空的批量Get列表
			 gets = getColumnIsEmptyGets(gids);
		 }else{
			 //获取列名不为空的批量Get列表
			 gets = getGets(gids);
		 }
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
		if (!docs.isEmpty()) {
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
	  		for(int i=0; i<docs.size(); ++i) {
	  			bulkRequestBuilder.add(client.prepareIndex(index, type, docs.get(i).first).setSource(docs.get(i).second));
	  		}
	  		
	  		long start = System.currentTimeMillis();
	  		BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
	  		if(bulkResponse.hasFailures()) {
	  			System.out.println(" faild messge : " + bulkResponse.buildFailureMessage());
	  		}
	  		
	  		System.out.println("[Upload docs number: " + docs.size() + "] [spent: " + (System.currentTimeMillis() - start) + "]");
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
	public void indexHBase(List<String> gids,Context context) {
		try {
			//根据gids列表，从Hbase中获取相关的列表信息，提取列名不为空的情况
			ArrayList<Pair> docs  = getUserProfileFromHBase(gids, false, context);
			indexDocs(docs);
			gids.clear();
			if (column != null && column.trim().length() > 0) {
				System.out.println("second index---"+hbase_table+":"+family+":"+column+" missed rowkeys size:"+missedrowkeys.size());
				if (missedrowkeys != null && missedrowkeys.size() > 0) {
					docs  = getUserProfileFromHBase(missedrowkeys,true,context);
					indexDocs(docs);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
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
          //初始化IndexUpOldInternetDimension
          IndexUpOldInternetDimension indexUpOldInternetDimension = new IndexUpOldInternetDimension(indexCategoryArrayObject,
                                                                                                    indexIntLevelAttribute,
                                                                                                    indexStringLevelAttribute);
          //初始化IndexUpMarketDimension
          IndexUpMarketDimension indexUpMarketDimension = new IndexUpMarketDimension(indexIntLevelAttribute);
          
          ParseProtobuf2JSON parseProtobuf2JSON = new ParseProtobuf2JSON(cid_fin,
				                                                         upDgDimension,
				                                                         upCategoryDimension,
                                                                         indexUpInternetDimension,
                                                                         indexUpMarketDimension,
                                                                         indexUpOldInternetDimension,
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
			if (gids.size() == scan_gid_size) {
				indexHBase(gids,context);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
