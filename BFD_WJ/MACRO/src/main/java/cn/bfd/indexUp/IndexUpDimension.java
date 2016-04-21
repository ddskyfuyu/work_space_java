package cn.bfd.indexUp;


import com.alibaba.fastjson.JSONObject;
import org.json.simple.JSONArray;

/**
 * Created by yu.fu on 2015/7/23.
 */
public interface IndexUpDimension {

     void fillUpDimension(Object object, JSONObject json);

     void fillUpDimension(Object object, JSONArray jsons);

     void fillUpDimension(Object object, JSONArray jsons, JSONObject json);

     void fillUpDimension(Object object, JSONArray jsons, String type);

     void setUpDimension(JSONObject up, JSONObject value, String key);

     void setUpDimension(JSONObject up, JSONArray value, String key);
}
