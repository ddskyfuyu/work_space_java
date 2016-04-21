package cn.bfd.es;

import cn.bfd.indexAttribue.IndexIntLevelAttribute;
import cn.bfd.indexAttribue.IndexLongLevelAttribute;
import cn.bfd.indexAttribue.IndexStringLevelAttribute;
import cn.bfd.utils.Pair;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 更新用户画像的维度分析数据，用来分析相关的相关的删除与添加操作
 * 用于数据银行
 * @author yu.fu
 * @date 2016.1.13
 *
 */

public class ES_DB_Mapper extends Mapper<LongWritable,Text,Text, Text>{

	private static Logger LOG = Logger.getLogger(ES_DB_Mapper.class);

	private Client client = null;    //es客户端
	private String cluster_name = null;    //es集群名称
	private String es_ip = null;    //es的IP
	private int es_port ;    //es的端口
	private String index = null;    //es索引名称
	private String type = null;    //es与索引名称相对应的索引类型
	
	private ParseText2JSON parseText = null;    //配置转化JSON的处理类型
	private List<String> lines= new ArrayList<String>();    //保存批量的原始数据

	private Text keyText = new Text();
	private Text valueText = new Text();


	private int scan_gid_size = 0;    //配置批量处理的个数
	private int column_size = 0;    //配置列处理的个数


	/**
	 * 在MR程序运行之前，初始化相关的参数配置
	 * @param context MR程序对应的上下文变量
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void setup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();

		//初始化批量个数
		scan_gid_size = Integer.valueOf(conf.get("scan_gid_size"));

		//初始化列的个数
		column_size = Integer.valueOf(conf.get("column_size"));

		//初始化Elasticsearch参数
		cluster_name = conf.get("cluster_name");
		index = conf.get("es_index");
		type = conf.get("es_type");
		es_ip = conf.get("es_ip");
		es_port = Integer.valueOf(conf.get("es_port"));
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", cluster_name).build();
	    client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress(es_ip,es_port));

		//初始化转化类
		parseText = getParseText2JSON();
	}

	/**
	 * MR结束之前，做最后的清洗操作
	 * @param context MR程序对应的上下文变量
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		if (lines.size() > 0) {
			updateES(lines,context);
		}
		if (client !=null) {
			client.close();
		}
	}


	/**
	 * 将原始数据转化为指定的Pair类型
	 * @param lines 保存原始数据的List列表
	 * @param context MR程序对应的上下文变量
	 * @return 返回Pair类型的list列表
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public ArrayList<Pair> getDocs(List<String> lines,Context context) throws InterruptedException,IOException{

		ArrayList<Pair> docs = new ArrayList<Pair>();

        for(String line : lines){

			//处理划分列数与指定列数不一致问题
			if(line.trim().split(",").length != column_size){
				keyText.set(line);
				valueText.set("");
				context.write(keyText, valueText);
				continue;
			}

			JSONObject doc = new JSONObject();
			parseText.parse2JSon(line, doc);

			//处理缺乏gid数据的问题
			if(doc.get("gid") == null){
				keyText.set(line);
				valueText.set("");
				context.write(keyText, valueText);
				continue;
			}
			docs.add(new Pair((String)doc.get("gid"), doc.toString()));
		}
		return docs;
	}


	/**
	 * 将docs批量插入到Elasticsearch中
	 * @param docs 保存的是最终的JSON对象
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
				LOG.info(" faild messge : " + bulkResponse.buildFailureMessage());
			}

			System.out.println("[Upload docs number: " + docs.size() + "] [spent: " + (System.currentTimeMillis() - start) + "]");
		}

	}


	/**
	 * 使用MR程序，将lines里的数据批量更新到对应的ES中
	 * @param lines 保存原始文件每条数据的列表
	 * @param context MR程序对应的上下文变量
	 */
	public void updateES(List<String> lines,Context context) {
		try {
			//根据lines列表，将转化为Pair列表，其中Pair.first保存的是gid, Pair.second保存的是插入ES的JSON对象
			ArrayList<Pair> docs = getDocs(lines,context);
			//批量插入ES
			indexDocs(docs);
			lines.clear();
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 初始化ParseText2JSON对应的配置变量
	 *
	 * @return 返回初始化后的ParseText2JSON的对象
	 */
	public ParseText2JSON getParseText2JSON(){
		IndexIntLevelAttribute indexIntLevelAttribute = new IndexIntLevelAttribute();
		IndexLongLevelAttribute indexLongLevelAttribute = new IndexLongLevelAttribute();
		IndexStringLevelAttribute indexStringLevelAttribute = new IndexStringLevelAttribute();

		ParseText2JSON parseText2JSON = new ParseText2JSON(indexIntLevelAttribute,
				                                           indexLongLevelAttribute,
				                                           indexStringLevelAttribute);

        
        return parseText2JSON;

	}

	/**
	 * 覆盖的map函数，批量处理相关的原始数据
	 * @param key 原始文件的偏移量
	 * @param value 原始文件中每行的内容
	 * @param context MR对应的上下文变量
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String gid = value.toString();
			lines.add(gid);
			if (lines.size() == scan_gid_size) {
				updateES(lines, context);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
