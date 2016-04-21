package cn.bfd.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by BFD_278 on 2015/10/15.
 */
public class GetInternetReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        List<String> strList = new ArrayList<String>();

        for( Text val : values){
            strList.add(val.toString());
        }
        result.set(StringUtils.join(strList, ","));
        context.write(key,result);

//        if("/长期购物偏好".equals(new String(key.getBytes(),"utf-8"))){
//            result.set(sum);
//            context.write(key, result);
//            result.set((int) (sum * 0.46));
//            context.write(new Text("/短期购物偏好"), result);
//            result.set((int) (sum * 0.18));
//            context.write(new Text("/当下需求特征"), result);
//            result.set((int) (sum * 0.23));
//            context.write(new Text("/潜在需求特征"), result);
//        }
//
//        if("/长期兴趣偏好".equals(new String(key.getBytes(),"utf-8"))){
//            result.set(sum);
//            context.write(key, result);
//            result.set((int) (sum *  0.63));
//            context.write(new Text("/短期兴趣偏好"), result);
//
//        }

    }
}
