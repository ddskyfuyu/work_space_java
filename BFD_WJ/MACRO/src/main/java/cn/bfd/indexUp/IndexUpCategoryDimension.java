package cn.bfd.indexUp;


import cn.bfd.indexAttribute.*;
import cn.bfd.protobuf.PortraitClass;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yu.fu on 2015/7/23.
 * 将用户画像的品类信息转化为对应的JSON对象
 *
 */
public class IndexUpCategoryDimension implements IndexUpDimension {


    private static Logger LOG = Logger.getLogger(IndexUpCategoryDimension.class);
	
    private IndexArrayObject indexArrayObject;
    private IndexIntAttribute indexIntAttribute;
    private IndexDoubleAttribute indexDoubleAttribute;
    private IndexStringAttribute indexStringAttribute;
    private IndexNestedObject indexNestedObject;
    private IndexNestedObject indexFirstNestedObject;
    private IndexNestedObject indexSecondNestedObject;
    private IndexNestedObject indexThirdNestedObject;
    private IndexNestedObject indexFourthNestedObject;
    private IndexNestedObject indexFifthNestedObject;


    /**
     * IndexUpCategoryDimension类的构造函数，用来初始化相关的
     * @param indexArrayObject IndexArrayObject 处理数组类型的类
     * @param indexIntAttribute indexIntAttribute 处理int类型的类
     * @param indexDoubleAttribute IndexDoubleAttribute 处理double类型的类
     * @param indexStringAttribute IndexStringAttribute 处理String类型的类
     * @param indexNestedObject IndexNestedObject 处理Nested对象的类
     * @param indexFirstNestedObject IndexNestedObject 处理一级品类Nested对象的类
     * @param indexSecondNestedObject IndexNestedObject 处理二级品类Nested对象的类
     * @param indexThirdNestedObject IndexNestedObject 处理三级品类Nested对象的类
     * @param indexFourthNestedObject IndexNestedObject 处理四级品类Nested对象的类
     * @param indexFifthNestedObject IndexNestedObject 处理五级品类Nested对象的类
     */
    public IndexUpCategoryDimension(IndexArrayObject indexArrayObject, IndexIntAttribute indexIntAttribute, IndexDoubleAttribute indexDoubleAttribute, IndexStringAttribute indexStringAttribute,
                                    IndexNestedObject indexNestedObject,IndexNestedObject indexFirstNestedObject, IndexNestedObject indexSecondNestedObject,
                                    IndexNestedObject indexThirdNestedObject, IndexNestedObject indexFourthNestedObject, IndexNestedObject indexFifthNestedObject){

        this.indexArrayObject = indexArrayObject;
        this.indexIntAttribute = indexIntAttribute;
        this.indexDoubleAttribute = indexDoubleAttribute;
        this.indexStringAttribute = indexStringAttribute;
        this.indexNestedObject = indexNestedObject;
        this.indexFirstNestedObject = indexFirstNestedObject;
        this.indexSecondNestedObject = indexSecondNestedObject;
        this.indexThirdNestedObject = indexThirdNestedObject;
        this.indexFourthNestedObject = indexFourthNestedObject;
        this.indexFifthNestedObject = indexFifthNestedObject;
    }

    /**
     * 设置处理数组的类
     * @param indexArrayObject IndexArrayObject 处理数组类的对象
     */
    public void setIndexArrayObject(IndexArrayObject indexArrayObject){
        this.indexArrayObject = indexArrayObject;
    }

    /**
     * 设置处理int类型的类
     * @param indexIntAttribute IndexIntAttribute 处理int类型类的对象
     */
    public void setIndexIntAttribute(IndexIntAttribute indexIntAttribute) {
        this.indexIntAttribute = indexIntAttribute;
    }

    /**
     * 设置处理String类型的类
     * @param indexStringAttribute IndexStringAttribute 处理String类型的对象
     */
    public void setIndexStringAttribute(IndexStringAttribute indexStringAttribute){
        this.indexStringAttribute = indexStringAttribute;
    }

    /**
     * 设置处理Nested类型的类
     * @param indexNestedObject IndexNestedObject 处理Nested类型的对象
     */
    public void setIndexNestedObject(IndexNestedObject indexNestedObject){
        this.indexNestedObject = indexNestedObject;
    }


    /**
     * 设置处理Nested类型的类
     * @param indexFirstNestedObject IndexNestedObject 处理一级品类Nested对象的类
     * @param indexSecondNestedObject IndexNestedObject 处理二级品类Nested对象的类
     * @param indexThirdNestedObject IndexNestedObject 处理三级品类Nested对象的类
     * @param indexFourthNestedObject IndexNestedObject 处理四级品类Nested对象的类
     * @param indexFifthNestedObject IndexNestedObject 处理五级品类Nested对象的类
     */
    public void setIndexNestedObject(IndexNestedObject indexFirstNestedObject,
                                     IndexNestedObject indexSecondNestedObject,
                                     IndexNestedObject indexThirdNestedObject,
                                     IndexNestedObject indexFourthNestedObject,
                                     IndexNestedObject indexFifthNestedObject){

        this.indexFirstNestedObject = indexFirstNestedObject;
        this.indexSecondNestedObject = indexSecondNestedObject;
        this.indexThirdNestedObject = indexThirdNestedObject;
        this.indexFourthNestedObject = indexFourthNestedObject;
        this.indexFifthNestedObject = indexFifthNestedObject;


    }

    /**
     * 将品类中挂载的对应属性转化成对应的JSON对象
     * @param obj JSONObject 品类中挂载的对应属性存储的JSON对象
     * @param attr UserProfile2.AttributeInfo 品类中挂载的对应属性
     */
    public void parseBfdCategory(JSONObject obj, PortraitClass.AttributeInfo attr) {
    	//max_price
    	if(attr.hasMaxPrice()){
    		indexDoubleAttribute.setIndexDoubleAttribute(obj, "max_price", attr.getMaxPrice().getValue());
        }
    	
    	//min_price
    	 if(attr.hasMinPrice()){
    		 indexDoubleAttribute.setIndexDoubleAttribute(obj, "min_price", attr.getMinPrice().getValue());
         }
    	 

         //average_price
         if(attr.hasMaxPrice() && attr.hasMinPrice()){
        	 indexDoubleAttribute.setIndexDoubleAttribute(obj, "average_price", (double) ((attr.getMinPrice().getValue() + attr.getMaxPrice().getValue()) / 2));
         }

    	 
    	 //brands
    	 JSONArray brands = new JSONArray();
    	 for (int i = 0; i < attr.getBrandsCount(); ++i) {
             brands.add(attr.getBrands(i).getValue());
         }
         if (!brands.isEmpty()) {
             indexArrayObject.setJSONObject(obj, brands, "brands");
         }

         if(attr.hasFood()){
             PortraitClass.Food food = attr.getFood();
             //tastes
             JSONArray tastes = new JSONArray();
             for (int i = 0; i < food.getTastesCount(); ++i) {
                 tastes.add(food.getTastes(i).getValue());
             }
             if (!tastes.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, tastes, "tastes");
             }

             //materials
             JSONArray materials = new JSONArray();
             for (int i = 0; i < food.getMaterialsCount(); ++i) {
                 materials.add(food.getMaterials(i).getValue());
             }
             if (!materials.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, materials, "materials");
             }

             //crowds
             JSONArray crowds = new JSONArray();
             for (int i = 0; i < food.getCrowdsCount(); ++i) {
                 crowds.add(food.getCrowds(i).getValue());
             }
             if (!crowds.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, crowds, "crowds");
             }

             //crafts
             JSONArray crafts = new JSONArray();
             for (int i = 0; i < food.getCraftsCount(); ++i) {
                 crafts.add(food.getCrafts(i).getValue());
             }
             if (!crafts.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, crafts, "crafts");
             }

             //diseases
             JSONArray diseases = new JSONArray();
             for (int i = 0; i < food.getDiseasesCount(); ++i) {
                 diseases.add(food.getDiseases(i).getValue());
             }
             if (!diseases.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, diseases, "diseases");
             }

             //visceras
             JSONArray visceras = new JSONArray();
             for (int i = 0; i < food.getViscerasCount(); ++i) {
                 visceras.add(food.getVisceras(i).getValue());
             }
             if (!visceras.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, visceras, "visceras");
             }

             //functions
             JSONArray functions = new JSONArray();
             for (int i = 0; i < food.getFunctionsCount(); ++i) {
                 functions.add(food.getFunctions(i).getValue());
             }
             if (!functions.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, functions, "functions");
             }

             //wares
             JSONArray wares = new JSONArray();
             for (int i = 0; i < food.getWaresCount(); ++i) {
                 wares.add(food.getWares(i).getValue());
             }
             if (!wares.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, wares, "wares");
             }

             //durations
             JSONArray durations = new JSONArray();
             for (int i = 0; i < food.getDurationsCount(); ++i) {
                 durations.add(food.getDurations(i).getValue());
             }
             if (!durations.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, durations, "durations");
             }

             //styles
             JSONArray styles = new JSONArray();
             for (int i = 0; i < food.getStylesCount(); ++i) {
                 styles.add(food.getStyles(i).getValue());
             }
             if (!styles.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, styles, "styles");
             }

             //difficultys
             JSONArray difficultys = new JSONArray();
             for (int i = 0; i < food.getDifficultysCount(); ++i) {
                 difficultys.add(food.getDifficultys(i).getValue());
             }
             if (!difficultys.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, difficultys, "difficultys");
             }

             //scenes
             JSONArray scenes = new JSONArray();
             for (int i = 0; i < food.getScenesCount(); ++i) {
                 scenes.add(food.getScenes(i).getValue());
             }
             if (!scenes.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, scenes, "scenes");
             }
         }

         if(attr.hasPhone()){
             //mb_model
             JSONArray mb_model = new JSONArray();
             PortraitClass.Phone phone = attr.getPhone();
             for (int i = 0; i < phone.getMbModelCount(); ++i) {
                 mb_model.add(phone.getMbModel(i).getValue());
             }
             if (!mb_model.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_model, "mb_model");
             }

             //mb_date
             JSONArray mb_date = new JSONArray();
             for (int i = 0; i < phone.getMbDateCount(); ++i) {
                 mb_date.add(phone.getMbDate(i).getValue());
             }
             if (!mb_date.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_date, "mb_date");
             }

             //mb_color
             JSONArray mb_color = new JSONArray();
             for (int i = 0; i < phone.getMbColorCount(); ++i) {
                 mb_color.add(phone.getMbColor(i).getValue());
             }
             if (!mb_color.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_color, "mb_color");
             }

             //mb_support
             JSONArray mb_support = new JSONArray();
             for (int i = 0; i < phone.getMbSupportCount(); ++i) {
                 mb_support.add(phone.getMbSupport(i).getValue());
             }
             if (!mb_support.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_support, "mb_support");
             }

             //mb_os
             JSONArray mb_os = new JSONArray();
             for (int i = 0; i < phone.getMbOsCount(); ++i) {
                 mb_os.add(phone.getMbOs(i).getValue());
             }
             if (!mb_os.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_os, "mb_os");
             }

             //mb_resolution
             JSONArray mb_resolution = new JSONArray();
             for (int i = 0; i < phone.getMbResolutionCount(); ++i) {
                 mb_resolution.add(phone.getMbResolution(i).getValue());
             }
             if (!mb_resolution.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_resolution, "mb_resolution");
             }

             //mb_ram
             JSONArray mb_ram = new JSONArray();
             for (int i = 0; i < phone.getMbRamCount(); ++i) {
                 mb_ram.add(phone.getMbRam(i).getValue());
             }
             if (!mb_ram.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_ram, "mb_ram");
             }

             //mb_rom
             JSONArray mb_rom = new JSONArray();
             for (int i = 0; i < phone.getMbRomCount(); ++i) {
                 mb_rom.add(phone.getMbRom(i).getValue());
             }
             if (!mb_rom.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_rom, "mb_rom");
             }

             //mb_camera
             JSONArray mb_camera = new JSONArray();
             for (int i = 0; i < phone.getMbCameraCount(); ++i) {
                 mb_camera.add(phone.getMbCamera(i).getValue());
             }
             if (!mb_camera.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_camera, "mb_camera");
             }

             //mb_screen
             JSONArray mb_screen = new JSONArray();
             for (int i = 0; i < phone.getMbScreenCount(); ++i) {
                 mb_screen.add(phone.getMbScreen(i).getValue());
             }
             if (!mb_screen.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_screen, "mb_screen");
             }

             //mb_pattern
             JSONArray mb_pattern = new JSONArray();
             for (int i = 0; i < phone.getMbPatternCount(); ++i) {
                 mb_pattern.add(phone.getMbPattern(i).getValue());
             }
             if (!mb_pattern.isEmpty()) {
                 indexArrayObject.setJSONObject(obj, mb_pattern, "mb_pattern");
             }
         }

        
    }


    public void fillUpDimension(Object object, JSONObject obj) {


    }

    /**
     * 将用户画像中的品类信息填充到JSON数组中
     * @param object Object 用户画像的品类对象
     * @param jsons JSONArray JSON数组，用来保存相关的品类信息
     */
    public void fillUpDimension(Object object, JSONArray jsons) {

        //将object转化为CidInfo类型
        if(!(object instanceof PortraitClass.CidInfo)){
            LOG.warn("The object is not PortraitClass.CidInfo object. ");
            return;
        }
        PortraitClass.CidInfo cidInfo = (PortraitClass.CidInfo) object;

        if(cidInfo != null){
            String cid = cidInfo.getCid();
            //长期购物偏好
            if(cidInfo.hasEcIndus()){
                PortraitClass.IndustryInfo ecIndus = cidInfo.getEcIndus();
                for(int index_first_category = 0; index_first_category < ecIndus.getFirstCateCount(); ++index_first_category){
                        PortraitClass.Category firstCategory = ecIndus.getFirstCate(index_first_category);
                        String firstCateName = firstCategory.getName();
                        Map<String, String> stringMap = new HashMap<String, String>();
                        stringMap.put("first_category", firstCateName);
                        stringMap.put("cid", cid);
                        Map<String, String> timeStampMap = new HashMap<String, String>();
                        Map<String, Map<String, String>> map = new HashMap<String,Map<String, String>>();
                        Map<String, String> IntegerMap = new HashMap<String, String>();
                        IntegerMap.put("type", "1");
                        map.put("String", stringMap);
                        map.put("TimeStamp", timeStampMap);
                        map.put("Integer",IntegerMap);

                        //一级品类偏好转化为对应的JSON
                        if (firstCategory.hasAttrs() || firstCategory.getNextCateCount() == 0) {
                            JSONObject first_obj = new JSONObject();
                            timeStampMap.put("update_time",String.valueOf(firstCategory.getUpdateTime()));
                            parseBfdCategory(first_obj, firstCategory.getAttrs());
                            indexFirstNestedObject.fillNestedObject(first_obj,map);
                            indexArrayObject.addArrayJSONObject(jsons,first_obj);
                        }

                        //二级品类偏好转化为对应的JSON
                        for (int index_second_category = 0; index_second_category < firstCategory.getNextCateCount(); ++index_second_category) {
                            PortraitClass.Category secondCategory = firstCategory.getNextCate(index_second_category);
                            stringMap.put("second_category", secondCategory.getName());
                            if (secondCategory.hasAttrs() || secondCategory.getNextCateCount() == 0) {
                                JSONObject second_obj = new JSONObject();
                                timeStampMap.put("update_time", String.valueOf(secondCategory.getUpdateTime()));
                                parseBfdCategory(second_obj, secondCategory.getAttrs());
                                indexSecondNestedObject.fillNestedObject(second_obj, map);
                                indexArrayObject.addArrayJSONObject(jsons, second_obj);
                            }

                            //三级品类偏好转化为对应的JSON
                            for (int index_third_category = 0; index_third_category < secondCategory.getNextCateCount(); ++index_third_category) {

                                PortraitClass.Category thirdCategory = secondCategory.getNextCate(index_third_category);
                                stringMap.put("third_category", thirdCategory.getName());
                                if (thirdCategory.hasAttrs() || thirdCategory.getNextCateCount() == 0) {
                                    JSONObject third_obj = new JSONObject();
                                    timeStampMap.put("update_time", String.valueOf(thirdCategory.getUpdateTime()));
                                    parseBfdCategory(third_obj, thirdCategory.getAttrs());
                                    indexThirdNestedObject.fillNestedObject(third_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, third_obj);
                                }

                                //四级品类偏好转化为对应的JSON
                                for (int index_fourth_category = 0; index_fourth_category < thirdCategory.getNextCateCount(); ++index_fourth_category) {
                                    PortraitClass.Category fourthCategory = thirdCategory.getNextCate(index_fourth_category);
                                    stringMap.put("fourth_category", fourthCategory.getName());
                                    if (fourthCategory.hasAttrs() || fourthCategory.getNextCateCount() == 0) {
                                        JSONObject fourth_obj = new JSONObject();
                                        timeStampMap.put("update_time", String.valueOf(fourthCategory.getUpdateTime()));
                                        parseBfdCategory(fourth_obj, fourthCategory.getAttrs());
                                        indexFourthNestedObject.fillNestedObject(fourth_obj, map);
                                        indexArrayObject.addArrayJSONObject(jsons, fourth_obj);
                                    }
                                    for (int index_five_category = 0; index_five_category < fourthCategory.getNextCateCount(); ++index_five_category) {
                                        PortraitClass.Category fiveCategory = fourthCategory.getNextCate(index_five_category);
                                        stringMap.put("fourth_category", fiveCategory.getName());
                                        JSONObject fifth_obj = new JSONObject();
                                        //五级品类偏好转化为对应的JSON
                                        timeStampMap.put("update_time", String.valueOf(fiveCategory.getUpdateTime()));
                                        if (fiveCategory.hasAttrs()) {
                                            parseBfdCategory(fifth_obj, fiveCategory.getAttrs());
                                        }
                                        indexFourthNestedObject.fillNestedObject(fifth_obj, map);
                                        indexArrayObject.addArrayJSONObject(jsons, fifth_obj);
                                    }
                                }
                            }
                        }
                }
            }

            //长期内容偏好
            if(cidInfo.hasMediaIndus()){
                PortraitClass.IndustryInfo mediaIndus = cidInfo.getMediaIndus();
                for(int index_first_category = 0; index_first_category < mediaIndus.getFirstCateCount(); ++index_first_category){
                    PortraitClass.Category firstCategory = mediaIndus.getFirstCate(index_first_category);
                    String firstCateName = firstCategory.getName();
                    Map<String, String> stringMap = new HashMap<String, String>();
                    stringMap.put("first_category", firstCateName);
                    stringMap.put("cid", cid);
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    Map<String, Map<String, String>> map = new HashMap<String,Map<String, String>>();
                    Map<String, String> IntegerMap = new HashMap<String, String>();
                    IntegerMap.put("type", "2");
                    map.put("String", stringMap);
                    map.put("TimeStamp", timeStampMap);
                    map.put("Integer",IntegerMap);

                    //一级品类偏好转化为对应的JSON
                    if (firstCategory.hasAttrs() || firstCategory.getNextCateCount() == 0) {
                        JSONObject first_obj = new JSONObject();
                        timeStampMap.put("update_time",String.valueOf(firstCategory.getUpdateTime()));
                        parseBfdCategory(first_obj, firstCategory.getAttrs());
                        indexFirstNestedObject.fillNestedObject(first_obj,map);
                        indexArrayObject.addArrayJSONObject(jsons,first_obj);
                    }

                    //二级品类偏好转化为对应的JSON
                    for (int index_second_category = 0; index_second_category < firstCategory.getNextCateCount(); ++index_second_category) {
                        PortraitClass.Category secondCategory = firstCategory.getNextCate(index_second_category);
                        stringMap.put("second_category", secondCategory.getName());
                        if (secondCategory.hasAttrs() || secondCategory.getNextCateCount() == 0) {
                            JSONObject second_obj = new JSONObject();
                            timeStampMap.put("update_time", String.valueOf(secondCategory.getUpdateTime()));
                            parseBfdCategory(second_obj, secondCategory.getAttrs());
                            indexSecondNestedObject.fillNestedObject(second_obj, map);
                            indexArrayObject.addArrayJSONObject(jsons, second_obj);
                        }

                        //三级品类偏好转化为对应的JSON
                        for (int index_third_category = 0; index_third_category < secondCategory.getNextCateCount(); ++index_third_category) {

                            PortraitClass.Category thirdCategory = secondCategory.getNextCate(index_third_category);
                            stringMap.put("third_category", thirdCategory.getName());
                            if (thirdCategory.hasAttrs() || thirdCategory.getNextCateCount() == 0) {
                                JSONObject third_obj = new JSONObject();
                                timeStampMap.put("update_time", String.valueOf(thirdCategory.getUpdateTime()));
                                parseBfdCategory(third_obj, thirdCategory.getAttrs());
                                indexThirdNestedObject.fillNestedObject(third_obj, map);
                                indexArrayObject.addArrayJSONObject(jsons, third_obj);
                            }

                            //四级品类偏好转化为对应的JSON
                            for (int index_fourth_category = 0; index_fourth_category < thirdCategory.getNextCateCount(); ++index_fourth_category) {
                                PortraitClass.Category fourthCategory = thirdCategory.getNextCate(index_fourth_category);
                                stringMap.put("fourth_category", fourthCategory.getName());
                                if (fourthCategory.hasAttrs() || fourthCategory.getNextCateCount() == 0) {
                                    JSONObject fourth_obj = new JSONObject();
                                    timeStampMap.put("update_time", String.valueOf(fourthCategory.getUpdateTime()));
                                    parseBfdCategory(fourth_obj, fourthCategory.getAttrs());
                                    indexFourthNestedObject.fillNestedObject(fourth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fourth_obj);
                                }
                                for (int index_five_category = 0; index_five_category < fourthCategory.getNextCateCount(); ++index_five_category) {
                                    PortraitClass.Category fiveCategory = fourthCategory.getNextCate(index_five_category);
                                    stringMap.put("fourth_category", fiveCategory.getName());
                                    JSONObject fifth_obj = new JSONObject();
                                    //五级品类偏好转化为对应的JSON
                                    timeStampMap.put("update_time", String.valueOf(fiveCategory.getUpdateTime()));
                                    if (fiveCategory.hasAttrs()) {
                                        parseBfdCategory(fifth_obj, fiveCategory.getAttrs());
                                    }
                                    indexFourthNestedObject.fillNestedObject(fifth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fifth_obj);
                                }
                            }
                        }
                    }
                }
            }

            //当下需求特征
            if(cidInfo.hasCurrentDemand()){
                PortraitClass.IndustryInfo currentDemand = cidInfo.getCurrentDemand();
                for(int index_first_category = 0; index_first_category < currentDemand.getFirstCateCount(); ++index_first_category){
                    PortraitClass.Category firstCategory = currentDemand.getFirstCate(index_first_category);
                    String firstCateName = firstCategory.getName();
                    Map<String, String> stringMap = new HashMap<String, String>();
                    stringMap.put("first_category", firstCateName);
                    stringMap.put("cid", cid);
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    Map<String, Map<String, String>> map = new HashMap<String,Map<String, String>>();
                    Map<String, String> IntegerMap = new HashMap<String, String>();
                    IntegerMap.put("type", "4");
                    map.put("String", stringMap);
                    map.put("TimeStamp", timeStampMap);
                    map.put("Integer",IntegerMap);

                    //一级品类偏好转化为对应的JSON
                    if (firstCategory.hasAttrs() || firstCategory.getNextCateCount() == 0) {
                        JSONObject first_obj = new JSONObject();
                        timeStampMap.put("update_time",String.valueOf(firstCategory.getUpdateTime()));
                        parseBfdCategory(first_obj, firstCategory.getAttrs());
                        indexFirstNestedObject.fillNestedObject(first_obj,map);
                        indexArrayObject.addArrayJSONObject(jsons,first_obj);
                    }

                    //二级品类偏好转化为对应的JSON
                    for (int index_second_category = 0; index_second_category < firstCategory.getNextCateCount(); ++index_second_category) {
                        PortraitClass.Category secondCategory = firstCategory.getNextCate(index_second_category);
                        stringMap.put("second_category", secondCategory.getName());
                        if (secondCategory.hasAttrs() || secondCategory.getNextCateCount() == 0) {
                            JSONObject second_obj = new JSONObject();
                            timeStampMap.put("update_time", String.valueOf(secondCategory.getUpdateTime()));
                            parseBfdCategory(second_obj, secondCategory.getAttrs());
                            indexSecondNestedObject.fillNestedObject(second_obj, map);
                            indexArrayObject.addArrayJSONObject(jsons, second_obj);
                        }

                        //三级品类偏好转化为对应的JSON
                        for (int index_third_category = 0; index_third_category < secondCategory.getNextCateCount(); ++index_third_category) {

                            PortraitClass.Category thirdCategory = secondCategory.getNextCate(index_third_category);
                            stringMap.put("third_category", thirdCategory.getName());
                            if (thirdCategory.hasAttrs() || thirdCategory.getNextCateCount() == 0) {
                                JSONObject third_obj = new JSONObject();
                                timeStampMap.put("update_time", String.valueOf(thirdCategory.getUpdateTime()));
                                parseBfdCategory(third_obj, thirdCategory.getAttrs());
                                indexThirdNestedObject.fillNestedObject(third_obj, map);
                                indexArrayObject.addArrayJSONObject(jsons, third_obj);
                            }

                            //四级品类偏好转化为对应的JSON
                            for (int index_fourth_category = 0; index_fourth_category < thirdCategory.getNextCateCount(); ++index_fourth_category) {
                                PortraitClass.Category fourthCategory = thirdCategory.getNextCate(index_fourth_category);
                                stringMap.put("fourth_category", fourthCategory.getName());
                                if (fourthCategory.hasAttrs() || fourthCategory.getNextCateCount() == 0) {
                                    JSONObject fourth_obj = new JSONObject();
                                    timeStampMap.put("update_time", String.valueOf(fourthCategory.getUpdateTime()));
                                    parseBfdCategory(fourth_obj, fourthCategory.getAttrs());
                                    indexFourthNestedObject.fillNestedObject(fourth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fourth_obj);
                                }
                                for (int index_five_category = 0; index_five_category < fourthCategory.getNextCateCount(); ++index_five_category) {
                                    PortraitClass.Category fiveCategory = fourthCategory.getNextCate(index_five_category);
                                    stringMap.put("fourth_category", fiveCategory.getName());
                                    JSONObject fifth_obj = new JSONObject();
                                    //五级品类偏好转化为对应的JSON
                                    timeStampMap.put("update_time", String.valueOf(fiveCategory.getUpdateTime()));
                                    if (fiveCategory.hasAttrs()) {
                                        parseBfdCategory(fifth_obj, fiveCategory.getAttrs());
                                    }
                                    indexFourthNestedObject.fillNestedObject(fifth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fifth_obj);
                                }
                            }
                        }
                    }
                }
            }

            //潜在需求特征
            if(cidInfo.hasPotentialDemand()){
                PortraitClass.IndustryInfo potentialDemand = cidInfo.getPotentialDemand();
                for(int index_first_category = 0; index_first_category < potentialDemand.getFirstCateCount(); ++index_first_category){
                    PortraitClass.Category firstCategory = potentialDemand.getFirstCate(index_first_category);
                    String firstCateName = firstCategory.getName();
                    Map<String, String> stringMap = new HashMap<String, String>();
                    stringMap.put("first_category", firstCateName);
                    stringMap.put("cid", cid);
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    Map<String, Map<String, String>> map = new HashMap<String,Map<String, String>>();
                    Map<String, String> IntegerMap = new HashMap<String, String>();
                    IntegerMap.put("type", "3");
                    map.put("String", stringMap);
                    map.put("TimeStamp", timeStampMap);
                    map.put("Integer",IntegerMap);

                    //一级品类偏好转化为对应的JSON
                    if (firstCategory.hasAttrs() || firstCategory.getNextCateCount() == 0) {
                        JSONObject first_obj = new JSONObject();
                        timeStampMap.put("update_time",String.valueOf(firstCategory.getUpdateTime()));
                        parseBfdCategory(first_obj, firstCategory.getAttrs());
                        indexFirstNestedObject.fillNestedObject(first_obj,map);
                        indexArrayObject.addArrayJSONObject(jsons,first_obj);
                    }

                    //二级品类偏好转化为对应的JSON
                    for (int index_second_category = 0; index_second_category < firstCategory.getNextCateCount(); ++index_second_category) {
                        PortraitClass.Category secondCategory = firstCategory.getNextCate(index_second_category);
                        stringMap.put("second_category", secondCategory.getName());
                        if (secondCategory.hasAttrs() || secondCategory.getNextCateCount() == 0) {
                            JSONObject second_obj = new JSONObject();
                            timeStampMap.put("update_time", String.valueOf(secondCategory.getUpdateTime()));
                            parseBfdCategory(second_obj, secondCategory.getAttrs());
                            indexSecondNestedObject.fillNestedObject(second_obj, map);
                            indexArrayObject.addArrayJSONObject(jsons, second_obj);
                        }

                        //三级品类偏好转化为对应的JSON
                        for (int index_third_category = 0; index_third_category < secondCategory.getNextCateCount(); ++index_third_category) {

                            PortraitClass.Category thirdCategory = secondCategory.getNextCate(index_third_category);
                            stringMap.put("third_category", thirdCategory.getName());
                            if (thirdCategory.hasAttrs() || thirdCategory.getNextCateCount() == 0) {
                                JSONObject third_obj = new JSONObject();
                                timeStampMap.put("update_time", String.valueOf(thirdCategory.getUpdateTime()));
                                parseBfdCategory(third_obj, thirdCategory.getAttrs());
                                indexThirdNestedObject.fillNestedObject(third_obj, map);
                                indexArrayObject.addArrayJSONObject(jsons, third_obj);
                            }

                            //四级品类偏好转化为对应的JSON
                            for (int index_fourth_category = 0; index_fourth_category < thirdCategory.getNextCateCount(); ++index_fourth_category) {
                                PortraitClass.Category fourthCategory = thirdCategory.getNextCate(index_fourth_category);
                                stringMap.put("fourth_category", fourthCategory.getName());
                                if (fourthCategory.hasAttrs() || fourthCategory.getNextCateCount() == 0) {
                                    JSONObject fourth_obj = new JSONObject();
                                    timeStampMap.put("update_time", String.valueOf(fourthCategory.getUpdateTime()));
                                    parseBfdCategory(fourth_obj, fourthCategory.getAttrs());
                                    indexFourthNestedObject.fillNestedObject(fourth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fourth_obj);
                                }
                                for (int index_five_category = 0; index_five_category < fourthCategory.getNextCateCount(); ++index_five_category) {
                                    PortraitClass.Category fiveCategory = fourthCategory.getNextCate(index_five_category);
                                    stringMap.put("fourth_category", fiveCategory.getName());
                                    JSONObject fifth_obj = new JSONObject();
                                    //五级品类偏好转化为对应的JSON
                                    timeStampMap.put("update_time", String.valueOf(fiveCategory.getUpdateTime()));
                                    if (fiveCategory.hasAttrs()) {
                                        parseBfdCategory(fifth_obj, fiveCategory.getAttrs());
                                    }
                                    indexFourthNestedObject.fillNestedObject(fifth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fifth_obj);
                                }
                            }
                        }
                    }
                }
            }

            //短期兴趣偏好
            if(cidInfo.hasTimelyMediaIndus()){
                PortraitClass.IndustryInfo timelyMediaIndus = cidInfo.getTimelyMediaIndus();
                for(int index_first_category = 0; index_first_category < timelyMediaIndus.getFirstCateCount(); ++index_first_category){
                    PortraitClass.Category firstCategory = timelyMediaIndus.getFirstCate(index_first_category);
                    String firstCateName = firstCategory.getName();
                    Map<String, String> stringMap = new HashMap<String, String>();
                    stringMap.put("first_category", firstCateName);
                    stringMap.put("cid", cid);
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    Map<String, Map<String, String>> map = new HashMap<String,Map<String, String>>();
                    Map<String, String> IntegerMap = new HashMap<String, String>();
                    IntegerMap.put("type", "5");
                    map.put("String", stringMap);
                    map.put("TimeStamp", timeStampMap);
                    map.put("Integer",IntegerMap);

                    //一级品类偏好转化为对应的JSON
                    if (firstCategory.hasAttrs() || firstCategory.getNextCateCount() == 0) {
                        JSONObject first_obj = new JSONObject();
                        timeStampMap.put("update_time",String.valueOf(firstCategory.getUpdateTime()));
                        parseBfdCategory(first_obj, firstCategory.getAttrs());
                        indexFirstNestedObject.fillNestedObject(first_obj,map);
                        indexArrayObject.addArrayJSONObject(jsons,first_obj);
                    }

                    //二级品类偏好转化为对应的JSON
                    for (int index_second_category = 0; index_second_category < firstCategory.getNextCateCount(); ++index_second_category) {
                        PortraitClass.Category secondCategory = firstCategory.getNextCate(index_second_category);
                        stringMap.put("second_category", secondCategory.getName());
                        if (secondCategory.hasAttrs() || secondCategory.getNextCateCount() == 0) {
                            JSONObject second_obj = new JSONObject();
                            timeStampMap.put("update_time", String.valueOf(secondCategory.getUpdateTime()));
                            parseBfdCategory(second_obj, secondCategory.getAttrs());
                            indexSecondNestedObject.fillNestedObject(second_obj, map);
                            indexArrayObject.addArrayJSONObject(jsons, second_obj);
                        }

                        //三级品类偏好转化为对应的JSON
                        for (int index_third_category = 0; index_third_category < secondCategory.getNextCateCount(); ++index_third_category) {

                            PortraitClass.Category thirdCategory = secondCategory.getNextCate(index_third_category);
                            stringMap.put("third_category", thirdCategory.getName());
                            if (thirdCategory.hasAttrs() || thirdCategory.getNextCateCount() == 0) {
                                JSONObject third_obj = new JSONObject();
                                timeStampMap.put("update_time", String.valueOf(thirdCategory.getUpdateTime()));
                                parseBfdCategory(third_obj, thirdCategory.getAttrs());
                                indexThirdNestedObject.fillNestedObject(third_obj, map);
                                indexArrayObject.addArrayJSONObject(jsons, third_obj);
                            }

                            //四级品类偏好转化为对应的JSON
                            for (int index_fourth_category = 0; index_fourth_category < thirdCategory.getNextCateCount(); ++index_fourth_category) {
                                PortraitClass.Category fourthCategory = thirdCategory.getNextCate(index_fourth_category);
                                stringMap.put("fourth_category", fourthCategory.getName());
                                if (fourthCategory.hasAttrs() || fourthCategory.getNextCateCount() == 0) {
                                    JSONObject fourth_obj = new JSONObject();
                                    timeStampMap.put("update_time", String.valueOf(fourthCategory.getUpdateTime()));
                                    parseBfdCategory(fourth_obj, fourthCategory.getAttrs());
                                    indexFourthNestedObject.fillNestedObject(fourth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fourth_obj);
                                }
                                for (int index_five_category = 0; index_five_category < fourthCategory.getNextCateCount(); ++index_five_category) {
                                    PortraitClass.Category fiveCategory = fourthCategory.getNextCate(index_five_category);
                                    stringMap.put("fourth_category", fiveCategory.getName());
                                    JSONObject fifth_obj = new JSONObject();
                                    //五级品类偏好转化为对应的JSON
                                    timeStampMap.put("update_time", String.valueOf(fiveCategory.getUpdateTime()));
                                    if (fiveCategory.hasAttrs()) {
                                        parseBfdCategory(fifth_obj, fiveCategory.getAttrs());
                                    }
                                    indexFourthNestedObject.fillNestedObject(fifth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fifth_obj);
                                }
                            }
                        }
                    }
                }
            }

            //短期购物偏好
            if(cidInfo.hasTimelyEcIndus()){
                PortraitClass.IndustryInfo timelyEcIndus = cidInfo.getTimelyEcIndus();
                for(int index_first_category = 0; index_first_category < timelyEcIndus.getFirstCateCount(); ++index_first_category){
                    PortraitClass.Category firstCategory = timelyEcIndus.getFirstCate(index_first_category);
                    String firstCateName = firstCategory.getName();
                    Map<String, String> stringMap = new HashMap<String, String>();
                    stringMap.put("first_category", firstCateName);
                    stringMap.put("cid", cid);
                    Map<String, String> timeStampMap = new HashMap<String, String>();
                    Map<String, Map<String, String>> map = new HashMap<String,Map<String, String>>();
                    Map<String, String> IntegerMap = new HashMap<String, String>();
                    IntegerMap.put("type", "6");
                    map.put("String", stringMap);
                    map.put("TimeStamp", timeStampMap);
                    map.put("Integer",IntegerMap);

                    //一级品类偏好转化为对应的JSON
                    if (firstCategory.hasAttrs() || firstCategory.getNextCateCount() == 0) {
                        JSONObject first_obj = new JSONObject();
                        timeStampMap.put("update_time",String.valueOf(firstCategory.getUpdateTime()));
                        parseBfdCategory(first_obj, firstCategory.getAttrs());
                        indexFirstNestedObject.fillNestedObject(first_obj,map);
                        indexArrayObject.addArrayJSONObject(jsons,first_obj);
                    }

                    //二级品类偏好转化为对应的JSON
                    for (int index_second_category = 0; index_second_category < firstCategory.getNextCateCount(); ++index_second_category) {
                        PortraitClass.Category secondCategory = firstCategory.getNextCate(index_second_category);
                        stringMap.put("second_category", secondCategory.getName());
                        if (secondCategory.hasAttrs() || secondCategory.getNextCateCount() == 0) {
                            JSONObject second_obj = new JSONObject();
                            timeStampMap.put("update_time", String.valueOf(secondCategory.getUpdateTime()));
                            parseBfdCategory(second_obj, secondCategory.getAttrs());
                            indexSecondNestedObject.fillNestedObject(second_obj, map);
                            indexArrayObject.addArrayJSONObject(jsons, second_obj);
                        }

                        //三级品类偏好转化为对应的JSON
                        for (int index_third_category = 0; index_third_category < secondCategory.getNextCateCount(); ++index_third_category) {

                            PortraitClass.Category thirdCategory = secondCategory.getNextCate(index_third_category);
                            stringMap.put("third_category", thirdCategory.getName());
                            if (thirdCategory.hasAttrs() || thirdCategory.getNextCateCount() == 0) {
                                JSONObject third_obj = new JSONObject();
                                timeStampMap.put("update_time", String.valueOf(thirdCategory.getUpdateTime()));
                                parseBfdCategory(third_obj, thirdCategory.getAttrs());
                                indexThirdNestedObject.fillNestedObject(third_obj, map);
                                indexArrayObject.addArrayJSONObject(jsons, third_obj);
                            }

                            //四级品类偏好转化为对应的JSON
                            for (int index_fourth_category = 0; index_fourth_category < thirdCategory.getNextCateCount(); ++index_fourth_category) {
                                PortraitClass.Category fourthCategory = thirdCategory.getNextCate(index_fourth_category);
                                stringMap.put("fourth_category", fourthCategory.getName());
                                if (fourthCategory.hasAttrs() || fourthCategory.getNextCateCount() == 0) {
                                    JSONObject fourth_obj = new JSONObject();
                                    timeStampMap.put("update_time", String.valueOf(fourthCategory.getUpdateTime()));
                                    parseBfdCategory(fourth_obj, fourthCategory.getAttrs());
                                    indexFourthNestedObject.fillNestedObject(fourth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fourth_obj);
                                }
                                for (int index_five_category = 0; index_five_category < fourthCategory.getNextCateCount(); ++index_five_category) {
                                    PortraitClass.Category fiveCategory = fourthCategory.getNextCate(index_five_category);
                                    stringMap.put("fourth_category", fiveCategory.getName());
                                    JSONObject fifth_obj = new JSONObject();
                                    //五级品类偏好转化为对应的JSON
                                    timeStampMap.put("update_time", String.valueOf(fiveCategory.getUpdateTime()));
                                    if (fiveCategory.hasAttrs()) {
                                        parseBfdCategory(fifth_obj, fiveCategory.getAttrs());
                                    }
                                    indexFourthNestedObject.fillNestedObject(fifth_obj, map);
                                    indexArrayObject.addArrayJSONObject(jsons, fifth_obj);
                                }
                            }
                        }
                    }
                }
            }

        }
    }

    public void fillUpDimension(Object object, JSONArray jsons, String type){

    }

    public void fillUpDimension(Object object, JSONArray jsons, JSONObject json){

    }

    public void setUpDimension(JSONObject up, JSONObject value, String key) {
        up.put(key, value);
    }

    public void setUpDimension(JSONObject up, JSONArray value, String key) {
        up.put(key,value);
    }

}
