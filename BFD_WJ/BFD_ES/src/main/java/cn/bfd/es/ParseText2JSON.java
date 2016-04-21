package cn.bfd.es;

import cn.bfd.indexAttribue.IndexIntAttribute;
import cn.bfd.indexAttribue.IndexLongAttribute;
import cn.bfd.indexAttribue.IndexStringAttribute;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Created by yu.fu on 2015/7/21.
 * 将对应的google protobuf对象转化为对应的JSON对象.
 *
 */
public class ParseText2JSON implements ParseAlgorithm {

    private static Logger LOG = Logger.getLogger(ParseText2JSON.class);
    private IndexLongAttribute indexLongAttribute;
    private IndexIntAttribute indexIntAttribute;
    private IndexStringAttribute indexStringAttribute;


    public ParseText2JSON(IndexIntAttribute indexIntAttribute,  IndexLongAttribute indexLongAttribute, IndexStringAttribute indexStringAttribute){
        this.indexIntAttribute = indexIntAttribute;
        this.indexLongAttribute = indexLongAttribute;
        this.indexStringAttribute = indexStringAttribute;
    }


    void setIndexStringAttribute(IndexStringAttribute indexStringAttribute){
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
     * 将指定的obj对象里的内容保存到json中
     * @param obj 保存原始一条记录的信息，类型为String类型
     * @param json 保存最终的JSON对象
     */
    public void parse2JSon(Object obj, JSONObject json){

        if(json == null){
            LOG.error("Input json is null");
            return;
        }

        //构建输入数据与ES关键字的映射关系
        String str = (String)obj;
        String[] items = str.trim().split(",");

        indexStringAttribute.setIndexStringAttribute(json, "gid", items[0]);
        indexIntAttribute.setIndexIntAttribute(json, "up.active", 1);
        for(int i = 1; i < items.length; ++i){
            String[] subItems = items[i].trim().split(":");
            if(subItems.length != 2){
                LOG.error("The size does not equal to 2. " + items[i]);
                continue;
            }

            String sub_key = subItems[0];
            String sub_val = subItems[1];

            if("up.update_time".equals(sub_key)){
                long currentTime = Long.valueOf(sub_val) * 1000L;
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                indexStringAttribute.setIndexStringAttribute(json, sub_key, d);
            } else{
                indexIntAttribute.setIndexIntAttribute(json, sub_key,Integer.valueOf(sub_val));
            }

        }

    }

    /**
     * 根据GID的label，为每个gid添加业界领袖，临时需求已经被废除
     * @param gidLabelMap Map<String, String> key是gid，value表示是否为业界领袖
     * @param obj Object 用户画像对象
     * @param json JSON 转化后的JSON对象
     * @deprecated 临时需求，已经被废除
     */
    public void parse2JSon(Map<String, String> gidLabelMap,Object obj, JSONObject json) {

    }

}
