package cn.bfd.es;

import cn.bfd.indexAttribute.IndexArrayObject;
import cn.bfd.indexAttribute.IndexIntAttribute;
import cn.bfd.indexAttribute.IndexLongAttribute;
import cn.bfd.indexAttribute.IndexStringAttribute;
import cn.bfd.indexUp.IndexUpDimension;
import cn.bfd.protobuf.PortraitClass;
import cn.bfd.utils.FileUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yu.fu on 2015/7/21.
 * 将对应的google protobuf对象转化为对应的JSON对象.
 *
 */
public class ParseProtobuf2JSON implements ParseAlgorithm {

    private static Logger LOG = Logger.getLogger(ParseProtobuf2JSON.class);

    private IndexStringAttribute indexStringAttribute;
    private IndexLongAttribute indexLongAttribute;
    private IndexIntAttribute indexIntAttribute;
    private IndexArrayObject indexArrayObject;

    private IndexUpDimension upDgDimension;
    private IndexUpDimension upCategoryDimension;
    private IndexUpDimension upInternetDimension;
    private IndexUpDimension upMarketDimension;
    private IndexUpDimension upOldInternetDimension;




    private Map<String, Integer> cidMap;


    /**
     * ParseProtobuf2JSON结构函数
     *
     * @param cid_fin 字符串 存放cid名字的文件.
     * @param upDgDimension IndexUpDimension 处理人口属性的对象.
     * @param upCategoryDimension IndexUpDimension 处理品类信息的对象.
     * @param upInternetDimension IndexUpDimension 处理上网特征的对象.
     * @param upMarketDimension IndexUpDimension 处理营销特征的对象.
     * @param fs FileSystem HDFD的路径信息.
     *
     */
    public ParseProtobuf2JSON(String cid_fin, IndexUpDimension upDgDimension, IndexUpDimension upCategoryDimension,
                              IndexUpDimension upInternetDimension, IndexUpDimension upMarketDimension, FileSystem fs){

        cidMap = new HashMap<String, Integer>();
        FileUtils.FillMap(cidMap, cid_fin, ",", fs);
        this.upDgDimension = upDgDimension;
        this.upCategoryDimension = upCategoryDimension;
        this.upInternetDimension = upInternetDimension;
        this.upMarketDimension = upMarketDimension;

    }

    /**
     * ParseProtobuf2JSON结构函数
     *
     * @param cid_fin 字符串 存放cid名字的文件.
     * @param upDgDimension IndexUpDimension 处理人口属性的对象.
     * @param upCategoryDimension IndexUpDimension 处理品类信息的对象.
     * @param upInternetDimension IndexUpDimension 处理上网特征的对象.
     * @param upMarketDimension IndexUpDimension 处理营销特征的对象.
     * @param upOldInternetDimension IndexUpDimension 处理上网特征的对象(旧对象，不按渠道进行划分).
     * @param fs FileSystem HDFD的路径信息.
     */
    public ParseProtobuf2JSON(String cid_fin, IndexUpDimension upDgDimension, IndexUpDimension upCategoryDimension,
                              IndexUpDimension upInternetDimension, IndexUpDimension upMarketDimension,
                              IndexUpDimension upOldInternetDimension, FileSystem fs){

        cidMap = new HashMap<String, Integer>();
        FileUtils.FillMap(cidMap, cid_fin, ",",fs);
        this.upDgDimension = upDgDimension;
        this.upCategoryDimension = upCategoryDimension;
        this.upInternetDimension = upInternetDimension;
        this.upMarketDimension = upMarketDimension;
        this.upOldInternetDimension = upOldInternetDimension;
    }


    /**
     * 设置String属性的处理对象
     *
     * @param indexStringAttribute IndexStringAttribute 处理String的具体对象
     */
    public void setIndexStringAttribute(IndexStringAttribute indexStringAttribute){
        this.indexStringAttribute = indexStringAttribute;
    }

    /**
     * 设置Long属性的处理对象
     *
     * @param indexLongAttribute IndexLongAttribute 处理Long的具体对象
     */
    public void setIndexLongAttribute(IndexLongAttribute indexLongAttribute){
        this.indexLongAttribute = indexLongAttribute;
    }

    /**
     * 设置Int属性的处理对象
     *
     * @param indexIntAttribute IndexIntAttribute 处理Int的具体对象
     */
    public void setIndexIntAttribute(IndexIntAttribute indexIntAttribute){
        this.indexIntAttribute = indexIntAttribute;
    }

    /**
     * 设置数组的处理对象
     *
     * @param indexArrayObject IndexIntAttribute 处理数组的具体对象
     */
    public void setIndexArrayObject(IndexArrayObject indexArrayObject){
        this.indexArrayObject = indexArrayObject;
    }

    /**
     * 将google protobuf对象转化为JSON对象
     *
     * @param obj Object对象 JSON对象
     * @param json JSONObject obj转化的JSON结果存储在json这个对象中
     */
    public void parse2JSon(Object obj, JSONObject json){
    	 if(obj == null || json == null){
             System.out.println("The input is wrong. ");
             return;
         }
        //将obj对象转化为protobuf对象
         PortraitClass.Portrait up = (PortraitClass.Portrait) obj;
         String gid = up.getUuid();
         indexStringAttribute.setIndexStringAttribute(json, "gid", gid);
         
         indexLongAttribute.setIndexLongAttribute(json,"update_time", Long.valueOf(up.getUpdateTime()));

         //填充用户画像-人口属性
         if(up.hasDgInfo()){
             upDgDimension.fillUpDimension(up.getDgInfo(),json);
         }

         //填充用户画像-上网特征维度
         JSONArray internet_jsons = new JSONArray();
         if(up.getInterCount() != 0){
             //上网特征按照渠道划分-PC，Mobile以及All
             upInternetDimension.fillUpDimension(up, internet_jsons);
         }
         if(internet_jsons.size() != 0){
             indexArrayObject.setJSONObject(json, internet_jsons, "inter_ft");
         }

         //用户画像品类偏好维度
         JSONArray category_jsons = new JSONArray();
         for(int i = 0; i < up.getCidInfoCount(); ++i){
             PortraitClass.CidInfo cidInfo = up.getCidInfo(i);
             String cid = cidInfo.getCid();
             //填充指定CID(客户)的品类偏好信息
             if(!cidMap.containsKey(cid)){
                 continue;
             }
             upCategoryDimension.fillUpDimension(cidInfo,category_jsons);
         }
         if(category_jsons.size() != 0){
             indexArrayObject.setJSONObject(json, category_jsons, "category");
         }

         //用户画像营销特征维度
         if(up.hasMarket()){
             upMarketDimension.fillUpDimension(up.getMarket(),json);
         }
    }


    public void parse2JSon(Map<String, Object> objMap, JSONObject json){
        if(objMap == null || json == null){
            LOG.error("The input is wrong. ");
            return;
        }
        //将obj对象转化为protobuf对象
        //PortraitClass.Portrait up = (PortraitClass.Portrait) obj;

        //根据不同的关键字转化对应的对象
        if(objMap.containsKey("gid")){
            String gid = (String)objMap.get("gid");
            indexStringAttribute.setIndexStringAttribute(json, "gid", gid);
        }
        else{
            LOG.error("The objMap object does not contain gid");
            return;
        }
        String global_gid =(String)objMap.get("gid");
        if(objMap.containsKey("update_time")){
            String update_time = (String)objMap.get("update_time");
            indexLongAttribute.setIndexLongAttribute(json, "update_time", Long.valueOf(update_time));
        }
        else{
            LOG.warn("The objMap object does not contain update_time. gid: " + global_gid);
        }



        //String gid = up.getUuid();
        //indexStringAttribute.setIndexStringAttribute(json, "gid", gid);

        //indexLongAttribute.setIndexLongAttribute(json,"update_time", Long.valueOf(up.getUpdateTime()));

        //填充用户画像-人口属性
        if(objMap.containsKey("demographic")){
            PortraitClass.DemographicInfo dgInfo = (PortraitClass.DemographicInfo)objMap.get("demographic");
            upDgDimension.fillUpDimension(dgInfo,json);
        }
        else{
            LOG.warn("The objMap object does not contain demographic. gid: " + global_gid);
        }
//        if(up.hasDgInfo()){
//            upDgDimension.fillUpDimension(up.getDgInfo(),json);
//        }

        //填充用户画像-PC端上网特征维度
        JSONArray internet_jsons = new JSONArray();
        if(objMap.containsKey("inet_PC")){
            upInternetDimension.fillUpDimension(objMap.get("inet_PC"), internet_jsons, "inet_PC");
        }
        else{
            LOG.warn("The objMap object does not contain inet_PC. gid: " + global_gid);
        }
        if(objMap.containsKey("inet_Mobile")){
            upInternetDimension.fillUpDimension(objMap.get("inet_Mobile"), internet_jsons, "inet_Mobile");
        }
        else{
            LOG.warn("The objMap object does not contain inet_Mobile. gid: " + global_gid);
        }
//
//        if(up.getInterCount() != 0){
//            //上网特征按照渠道划分-PC，Mobile以及All
//            upInternetDimension.fillUpDimension(up, internet_jsons);
//        }
        if(internet_jsons.size() != 0){
            indexArrayObject.setJSONObject(json, internet_jsons, "inter_ft");
        }

        //用户画像品类偏好维度
        JSONArray category_jsons = new JSONArray();
        if(objMap.containsKey("cid_Cbaifendian")){
            PortraitClass.CidInfo cidInfo = (PortraitClass.CidInfo)objMap.get("cid_Cbaifendian");
            upCategoryDimension.fillUpDimension(cidInfo,category_jsons);
        }
        else{
            LOG.warn("The objMap object does not contain cid_Cbaifendian. gid: " + global_gid);
        }

//        for(int i = 0; i < up.getCidInfoCount(); ++i){
//            PortraitClass.CidInfo cidInfo = up.getCidInfo(i);
//            String cid = cidInfo.getCid();
//            //填充指定CID(客户)的品类偏好信息
//            if(!cidMap.containsKey(cid)){
//                continue;
//            }
//            upCategoryDimension.fillUpDimension(cidInfo,category_jsons);
//        }
        //if(category_jsons.size() != 0){
        //    indexArrayObject.setJSONObject(json, category_jsons, "category");
        //}

        if(objMap.containsKey("market")){
            PortraitClass.MarketingFeatures mk_ft = (PortraitClass.MarketingFeatures)objMap.get("market");
            upMarketDimension.fillUpDimension(mk_ft,category_jsons,json);
        }
        else{
            LOG.warn("The objMap object does not contain market. gid: " + global_gid);
        }

        if(category_jsons.size() != 0){
            indexArrayObject.setJSONObject(json, category_jsons, "category");
        }
//        if(up.hasMarket()){
//            upMarketDimension.fillUpDimension(up.getMarket(),json);
//        }
    }

    /**
     * 根据GID的label，为每个gid添加业界领袖，临时需求已经被废除
     * @param gidLabelMap Map<String, String> key是gid，value表示是否为业界领袖
     * @param obj Object 用户画像对象
     * @param json JSON 转化后的JSON对象
     * @deprecated 临时需求，已经被废除
     */
    public void parse2JSon(Map<String, String> gidLabelMap,Object obj, JSONObject json) {
        if(obj == null || json == null){
            System.out.println("The input is wrong. ");
            return;
        }
        PortraitClass.Portrait up = (PortraitClass.Portrait) obj;
        String gid = up.getUuid();
        indexStringAttribute.setIndexStringAttribute(json, "gid", gid);
        String label = gidLabelMap.get(gid);
        
        if (label.equals("1")) {
        	indexIntAttribute.setIndexIntAttribute(json, "opinion_leader", 0);
		}else if (label.equals("2")) {
			indexIntAttribute.setIndexIntAttribute(json, "opinion_leader", 1);
		}
        indexLongAttribute.setIndexLongAttribute(json,"update_time", Long.valueOf(up.getUpdateTime()));

        if(up.hasDgInfo()){
            upDgDimension.fillUpDimension(up.getDgInfo(),json);
        }

        //用户画像-上网特征维度
        JSONArray internet_jsons = new JSONArray();
        if(up.getInterCount() != 0){
            //上网特征按照渠道划分-PC，Mobile以及All
            upInternetDimension.fillUpDimension(up, internet_jsons);
        }
        if(internet_jsons.size() != 0){
            indexArrayObject.setJSONObject(json, internet_jsons, "inter_ft");
        }

        //用户画像品类偏好维度
        JSONArray category_jsons = new JSONArray();
        for(int i = 0; i < up.getCidInfoCount(); ++i){
            cn.bfd.protobuf.PortraitClass.CidInfo cidInfo = up.getCidInfo(i);
            String cid = cidInfo.getCid();
            //填充指定CID(客户)的品类偏好信息
            if(!cidMap.containsKey(cid)){
                continue;
            }
            upCategoryDimension.fillUpDimension(cidInfo,category_jsons);
        }
        if(category_jsons.size() != 0){
            indexArrayObject.setJSONObject(json, category_jsons, "category");
        }

        //用户画像营销特征维度,level1
        if(up.hasMarket()){
            upMarketDimension.fillUpDimension(up.getMarket(),json);
        }
  }

}
