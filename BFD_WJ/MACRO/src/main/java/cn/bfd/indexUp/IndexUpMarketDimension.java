package cn.bfd.indexUp;


import cn.bfd.indexAttribute.*;
import cn.bfd.protobuf.PortraitClass;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yu.fu on 2015/7/23.
 * 将营销特征类转化为对应的JSON对象
 */

public class IndexUpMarketDimension implements IndexUpDimension {
    private static Logger LOG = Logger.getLogger(IndexUpMarketDimension.class);
    private IndexIntAttribute indexIntAttribute;
    private IndexStringAttribute indexStringAttribute;
    private IndexDoubleAttribute indexDoubleAttribute;
    private IndexNestedObject indexNestedObject;
    private IndexArrayObject indexArrayObject;

    /**
     * IndexUpMarketDimension结构函数
     * @param indexIntAttribute IndexIntAttribute 处理Int类型的对象
     */
    public IndexUpMarketDimension(IndexIntAttribute indexIntAttribute){
        this.indexIntAttribute = indexIntAttribute;
    }

    /**
     * IndexUpMarketDimension结构函数
     * @param indexIntAttribute IndexIntAttribute 处理Int类型对象
     * @param indexStringAttribute IndexStringAttribute 处理String类型对象
     */
    public IndexUpMarketDimension(IndexIntAttribute indexIntAttribute, IndexStringAttribute indexStringAttribute){
        this.indexIntAttribute = indexIntAttribute;
        this.indexStringAttribute = indexStringAttribute;
    }

    /**
     * IndexUpMarketDimension结构函数
     * @param indexIntAttribute IndexIntAttribute 处理Int类型对象
     * @param indexStringAttribute IndexStringAttribute 处理String类型对象
     * @param indexDoubleAttribute IndexDoubleAttribute 处理double类型对象
     */
    public IndexUpMarketDimension(IndexIntAttribute indexIntAttribute, IndexStringAttribute indexStringAttribute, IndexDoubleAttribute indexDoubleAttribute){
        this.indexIntAttribute = indexIntAttribute;
        this.indexStringAttribute = indexStringAttribute;
        this.indexDoubleAttribute = indexDoubleAttribute;
    }

    public IndexUpMarketDimension(IndexIntAttribute indexIntAttribute,
                                  IndexStringAttribute indexStringAttribute,
                                  IndexDoubleAttribute indexDoubleAttribute,
                                  IndexNestedObject indexNestedObject){
        this.indexIntAttribute = indexIntAttribute;
        this.indexStringAttribute = indexStringAttribute;
        this.indexDoubleAttribute = indexDoubleAttribute;
        this.indexNestedObject = indexNestedObject;
    }


    public IndexUpMarketDimension(IndexIntAttribute indexIntAttribute,
                                  IndexStringAttribute indexStringAttribute,
                                  IndexDoubleAttribute indexDoubleAttribute,
                                  IndexNestedObject indexNestedObject,
                                  IndexArrayObject indexArrayObject){
        this.indexIntAttribute = indexIntAttribute;
        this.indexStringAttribute = indexStringAttribute;
        this.indexDoubleAttribute = indexDoubleAttribute;
        this.indexNestedObject = indexNestedObject;
        this.indexArrayObject = indexArrayObject;
    }

    /**
     * 设置处理Int类型的对象
     * @param indexIntAttribute IndexIntAttribute 处理Int类型的对象
     */
    public void setIndexIntAttribute(IndexIntAttribute indexIntAttribute){
        this.indexIntAttribute = indexIntAttribute;
    }

    /**
     * 设置处理String类型的对象
     * @param indexStringAttribute IndexStringAttribute 处理String类型的对象
     */
    public void setIndexStringAttribute(IndexStringAttribute indexStringAttribute){
        this.indexStringAttribute = indexStringAttribute;
    }

    /**
     * 设置处理Double类型的对象
     * @param indexDoubleAttribute IndexDoubleAttribute 处理Double类型的对象
     */
    public void setIndexDoubleAttribute(IndexDoubleAttribute indexDoubleAttribute){
        this.indexDoubleAttribute = indexDoubleAttribute;
    }

    /**
     * 将SInfo链表中的对象value保存到对应的list中
     * @param list List<String> 存放结果的list
     * @param sInfos List<UserProfile2.SInfo> 存放SInfo的链表对象
     */
    private void buildListBySInfo(List<String> list, List<PortraitClass.SInfo> sInfos){
        for(PortraitClass.SInfo sInfo : sInfos){
            list.add(sInfo.getValue());
        }
    }

    /**
     * 设置处理Nested类型的类
     * @param indexNestedObject IndexNestedObject 处理Nested类型的对象
     */
    public void setIndexNestedObject(IndexNestedObject indexNestedObject){
        this.indexNestedObject = indexNestedObject;
    }

    /**
     * 设置处理数组的类
     * @param indexArrayObject IndexArrayObject 处理数组类的对象
     */
    public void setIndexArrayObject(IndexArrayObject indexArrayObject){
        this.indexArrayObject = indexArrayObject;
    }



    /**
     * 将营销特征转化为对应的JSON对象
     * @param object Object 营销特征对象
     * @param json JSON 转化后的JSON对象
     */
    public void fillUpDimension(Object object, JSONObject json) {

        if(!(object instanceof PortraitClass.MarketingFeatures)){
            return;
        }
        PortraitClass.MarketingFeatures mk_ft = (PortraitClass.MarketingFeatures)object;

        //营销价值-消费周期 con_periods
//        if (mk_ft.hasConPeriods()) {
//            List<Integer> list = new ArrayList<Integer>();
//            for (PortraitClass.IInfo iInfo : mk_ft.getConPeriodNewList()) {
//                list.add(iInfo.getValue());
//            }
//            if(list.size() != 0){
//                json.put("market_info.cyc_info", list);
//            }
//        }
        //营销价值-消费周期 con_periods
        if(mk_ft.hasConPeriods()){
            List<Integer> list = new ArrayList<Integer>();
            list.add(mk_ft.getConCapacity().getValue());
            json.put("market_info.cyc_info", list);
        }

        //营销价值-消费层级 price_level
        if(mk_ft.hasPriceLevel()){
            indexIntAttribute.setIndexIntAttribute(json,"market_info.price_level",Integer.valueOf(mk_ft.getPriceLevel().getValue()));
        }

        //营销价值-营销活动接收度
        if(mk_ft.hasMarkAccept()){
            indexStringAttribute.setIndexStringAttribute(json, "market_info.market_accept", mk_ft.getMarkAccept().getValue());
        }

        //营销价值-消费能力
        if(mk_ft.hasConCapacity()){
            indexIntAttribute.setIndexIntAttribute(json, "market_info.con_capacity", mk_ft.getConCapacity().getValue());
        }

        //营销价值-价格敏感度
        if(mk_ft.hasPriceSensitive()){
            indexDoubleAttribute.setIndexDoubleAttribute(json, "market_info.price_sensitive", mk_ft.getPriceSensitive().getValue());
        }










    }

    public void fillUpDimension(Object object, JSONArray jsons, JSONObject json){

        if(!(object instanceof PortraitClass.MarketingFeatures)){
            LOG.warn("The object is not PortraitClass.MarketingFeatures object. ");
            return;
        }
        PortraitClass.MarketingFeatures mk_ft = (PortraitClass.MarketingFeatures)object;
        String cid = "Cbaifendian";

        //营销价值-消费周期 con_periods
        if(mk_ft.hasConPeriods()){
            List<Integer> list = new ArrayList<Integer>();
            list.add(mk_ft.getConCapacity().getValue());
            json.put("market_info.cyc_info", list);
        }

        //营销价值-消费层级 price_level
        if(mk_ft.hasPriceLevel()){
            indexIntAttribute.setIndexIntAttribute(json,"market_info.price_level",Integer.valueOf(mk_ft.getPriceLevel().getValue()));
        }

        //营销价值-营销活动接收度
        if(mk_ft.hasMarkAccept()){
            indexStringAttribute.setIndexStringAttribute(json, "market_info.market_accept", mk_ft.getMarkAccept().getValue());
        }

        //营销价值-消费能力
        if(mk_ft.hasConCapacity()){
            indexIntAttribute.setIndexIntAttribute(json, "market_info.con_capacity", mk_ft.getConCapacity().getValue());
        }

        //营销价值-价格敏感度
        if(mk_ft.hasPriceSensitive()){
            indexDoubleAttribute.setIndexDoubleAttribute(json, "market_info.price_sensitive", mk_ft.getPriceSensitive().getValue());
        }
        //营销价值-添加购物车信息
        if(mk_ft.getAddcartCount() != 0){
            for(int i = 0; i < mk_ft.getAddcartCount(); ++i){
                PortraitClass.PayInfo payInfo = mk_ft.getAddcart(i);
                if(payInfo.hasName()){
                    String[] cates = payInfo.getName().split("\\$");
                    //按Map的不同类型存放其对应的类型以及对应实际Map对象
                    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
                    //存放时间戳的Map类型
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    //存放字符串的Map类型
                    Map<String, String> stringMap = new HashMap<String, String>();
                    //存放Int类型的Map类型
                    Map<String, String> integerMap = new HashMap<String, String>();

                    timeStampMap.put("update_time",String.valueOf(payInfo.getUpdateTime()));
                    //AddCart行为映射为2
                    integerMap.put("method", "2");

                    stringMap.put("cid", cid);
                    if (cates.length > 0) {
                        stringMap.put("first_category", cates[0]);
                    }
                    if (cates.length > 1) {
                        stringMap.put("second_category", cates[1]);
                    }
                    if (cates.length > 2) {
                        stringMap.put("third_category", cates[2]);
                    }
                    if(timeStampMap.size() > 0){
                        map.put("TimeStamp", timeStampMap);
                    }
                    if(stringMap.size() > 0){
                        map.put("String", stringMap);
                    }
                    if(integerMap.size() > 0){
                        map.put("Integer", integerMap);
                    }

                    if(map.size() > 0){
                        JSONObject add_json = new JSONObject();
                        indexNestedObject.fillNestedObject(add_json,map);
                        indexArrayObject.addArrayJSONObject(jsons, add_json);
                    }
                }
            }
        }

        //营销价值-添加购买信息
        if(mk_ft.getOrderCount() != 0){
            for(int i = 0; i < mk_ft.getOrderCount(); ++i){
                PortraitClass.PayInfo payInfo = mk_ft.getOrder(i);
                if(payInfo.hasName()){
                    String[] cates = payInfo.getName().split("\\$");
                    //按Map的不同类型存放其对应的类型以及对应实际Map对象
                    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
                    //存放时间戳的Map类型
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    //存放字符串的Map类型
                    Map<String, String> stringMap = new HashMap<String, String>();
                    //存放Int类型的Map类型
                    Map<String, String> integerMap = new HashMap<String, String>();

                    timeStampMap.put("update_time",String.valueOf(payInfo.getUpdateTime()));
                    //AddCart行为映射为2
                    integerMap.put("method", "1");

                    stringMap.put("cid", cid);
                    if (cates.length > 0) {
                        stringMap.put("first_category", cates[0]);
                    }
                    if (cates.length > 1) {
                        stringMap.put("second_category", cates[1]);
                    }
                    if (cates.length > 2) {
                        stringMap.put("third_category", cates[2]);
                    }

                    if(timeStampMap.size() > 0){
                        map.put("TimeStamp", timeStampMap);
                    }
                    if(stringMap.size() > 0){
                        map.put("String", stringMap);
                    }

                    if(integerMap.size() > 0){
                        map.put("Integer", integerMap);
                    }

                    if(map.size() > 0){
                        JSONObject order_json = new JSONObject();
                        indexNestedObject.fillNestedObject(order_json,map);
                        indexArrayObject.addArrayJSONObject(jsons, order_json);
                    }
                }
            }
        }

    }

    public void fillUpDimension(Object object, JSONArray jsons) {

    }

    public void fillUpDimension(Object object, JSONArray jsons, String type){

    }


    public void setUpDimension(JSONObject up, JSONObject value, String key) {

    }

    public void setUpDimension(JSONObject up, JSONArray value, String key) {

    }
}


