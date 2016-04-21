package cn.bfd.es;


import cn.bfd.protobuf.UserProfile2;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by BFD_278 on 2015/7/19.
 */
interface WorkFlow {
    void Process(UserProfile2.UserProfile up, JSONObject result);
}
