package cn.bfd.indexUp;

import cn.bfd.indexAttribue.IndexBooleanAttribute;
import cn.bfd.indexAttribue.IndexIntAttribute;
import cn.bfd.indexAttribue.IndexStringAttribute;
import cn.bfd.protobuf.UserProfile2;
import cn.bfd.utils.FileUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.json.simple.JSONArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yu.fu on 2015/7/23.
 * 完成人口属性的填充功能
 */

public class IndexUpDgDimension implements IndexUpDimension {

    private IndexBooleanAttribute indexBooleanAttribute;
    private IndexStringAttribute indexStringAttribute;
    private IndexIntAttribute indexIntAttribute;

    private Map<String, Integer> ageMap;
    private Map<String, Integer> sexMap;

    /**
     * 设置布尔属性的处理对象
     * @param indexBooleanAttribute 布尔属性的处理对象.
     */
    public void setIndexBooleanAttribute(IndexBooleanAttribute indexBooleanAttribute){
        this.indexBooleanAttribute = indexBooleanAttribute;
    }

    /**
     * 设置String属性的处理对象
     * @param indexStringAttribute String属性的处理对象
     */
    public void setIndexStringAttribute(IndexStringAttribute indexStringAttribute){
        this.indexStringAttribute = indexStringAttribute;
    }

    /**
     * 设置Int属性的处理对象
     * @param indexIntAttribute Index属性的处理对象
     */
    public void setIndexIntAttribute(IndexIntAttribute indexIntAttribute){
        this.indexIntAttribute = indexIntAttribute;
    }

    /**
     * IndexUpDgDimension对应的构造函数
     * @param indexBooleanAttribute IndexBooleanAttribute 初始化对应的布尔值处理对象
     * @param indexStringAttribute IndexStringAttribute 初始化对应的字符串处理对象
     * @param indexIntAttribute IndexIntAttribute 初始化对应的Int处理对象
     * @param age_fin 字符串 年龄映射文件
     * @param sex_fin 字符串 性别映射文件
     * @param fs 字符串 HDFS路径
     */
    public IndexUpDgDimension(IndexBooleanAttribute indexBooleanAttribute, IndexStringAttribute indexStringAttribute,
                              IndexIntAttribute indexIntAttribute, String age_fin, String sex_fin,FileSystem fs){

        //选择设置相关属性的处理对象类
        setIndexBooleanAttribute(indexBooleanAttribute);
        setIndexStringAttribute(indexStringAttribute);
        setIndexIntAttribute(indexIntAttribute);

        System.out.println("Age Path: " + age_fin);
        System.out.println("Sex Path: " + sex_fin);
        ageMap = new HashMap<String, Integer>();
        sexMap = new HashMap<String, Integer>();
        FileUtils.FillValMap(ageMap, age_fin, ",", fs);
        FileUtils.FillValMap(sexMap, sex_fin, ",", fs);
    }

    /**
     * 将用户画像的人口属性转化为JSON
     * @param object Object 用户画像中的人口属性对象
     * @param json JSONObject 保存最终的JSON结果
     */
    public void fillUpDimension(Object object, JSONObject json) {

        if(!(object instanceof UserProfile2.DemographicInfo)){
            return;
        }
        UserProfile2.DemographicInfo dg_info = (UserProfile2.DemographicInfo)object;

        //填充人口属性值-是否有孩子：dg_info.children,取值为1
        if(dg_info.hasHasBaby()){
            indexBooleanAttribute.setIndexBooleanLevelAttribute(json, "dg_info.children", dg_info.getHasBaby());
        }

        //填充人口属性值-是否结婚: dg_info.marriage,取值为1
        if(dg_info.hasMarried()){
            indexBooleanAttribute.setIndexBooleanLevelAttribute(json, "dg_info.marriage", dg_info.getMarried());
        }

        //填充人口属性值-城市和省份: dg_info.city dg_info.province
        if(dg_info.getCityCount() != 0){
            String location = dg_info.getCity(0).getValue();
            String[] array = location.split("\\$");
            //人口属性中city的字段组成：省$市
            if(array.length == 2) {
                indexStringAttribute.setIndexStringAttribute(json, "dg_info.province", array[0]);
                indexStringAttribute.setIndexStringAttribute(json, "dg_info.city", array[1]);
            }
            //人口属性中city的字段组成：国家$省$市 或者 国家$省$市$街道
            if(array.length == 3 || array.length == 4){
                indexStringAttribute.setIndexStringAttribute(json, "dg_info.province", array[1]);
                indexStringAttribute.setIndexStringAttribute(json, "dg_info.city", array[2]);
            }
        }

        //填充人口属性值-互联网年龄: dg_info.internet_age
        if(dg_info.getAgesCount() != 0){
            if(ageMap.containsKey(dg_info.getAges(0).getValue())){
                indexIntAttribute.setIndexIntAttribute(json, "dg_info.internet_age", Integer.valueOf(ageMap.get(dg_info.getAges(0).getValue())));
            }
        }

        //填充人口属性值-互联网年龄: dg_info.internet_sex
        if(dg_info.getSexCount() != 0){
            if(sexMap.containsKey(dg_info.getSex(0).getValue())){
                indexIntAttribute.setIndexIntAttribute(json, "dg_info.internet_sex", Integer.valueOf(sexMap.get(dg_info.getSex(0).getValue())));
            }
        }

        //填充人口属性值-生物性别年龄: dg_info.natural_age
        if(dg_info.hasBioAge()){
            if(ageMap.containsKey(dg_info.getBioAge().getValue())){
                indexIntAttribute.setIndexIntAttribute(json, "dg_info.natural_age", Integer.valueOf(ageMap.get(dg_info.getBioAge().getValue())));
            }
        }

        //填充人口属性值-生物性别: dg_info.natural_sex
        if(dg_info.hasBioGender()){
            if(sexMap.containsKey(dg_info.getBioGender().getValue())){
                indexIntAttribute.setIndexIntAttribute(json, "dg_info.natural_sex", Integer.valueOf(sexMap.get(dg_info.getBioGender().getValue())));
            }
        }


    }

    public void fillUpDimension(Object object, JSONArray jsons) {

    }


    public void setUpDimension(JSONObject up, JSONObject value, String key) {

    }

    public void setUpDimension(JSONObject up, JSONArray value, String key) {

    }
}


