package cn.bfd.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by BFD_278 on 2015/10/15.
 */
public class ManageReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }
        result.set(sum);
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
