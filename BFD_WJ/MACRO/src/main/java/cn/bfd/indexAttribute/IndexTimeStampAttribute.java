package cn.bfd.indexAttribute;


import com.alibaba.fastjson.JSONObject;

/**
 * Created by yu.fu on 2015/7/21.
 */
public interface IndexTimeStampAttribute {
    void setIndexTimeStampAttribute(JSONObject obj, String key, long value);
}

