package cn.bfd.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 * Created by yu.fu on 2015/10/15.
 */
public class GetPatchUpParamsAttribute {


    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        //set parameters
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 16){
            System.out.println("Usage: <table> <family> <reduceNum> <scan_gid_num> <input> <output> <attribute cache> <internet_time cache> <online_time cache> <frequent cache>"  +
                                      "<terminal_type cache> <dg_info cache> <age_level cache> <city_level cache> <max_split_size> <min_split_size>");
            return;
        }

        conf.set("mapred.textoutputformat.separator", "\t");
        conf.set("table", otherArgs[0]);
        conf.set("family", otherArgs[1]);
        conf.set("scan_gid_size", otherArgs[3]);


        //hbase old configuration
        //conf.set("hbase.zookeeper.quorum", "192.168.112.11,192.168.112.12,192.168.112.13");
        //conf.set("zookeeper.znode.parent", "/bfdhbase");
        //conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");

        //conf.set("hbase.zookeeper.quorum", "192.168.50.11,192.168.50.12,192.168.50.13,192.168.50.14,192.168.50.15");
        conf.set("hbase.zookeeper.quorum", "192.168.49.203,192.168.49.204,192.168.49.205");
        conf.set("zookeeper.znode.parent", "/bfdhbasehot");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");


        //job configuration
        //Job job = new Job(conf, "GetPatchAttributeMain");
        Job job = Job.getInstance(conf,"GetPatchAttributeMain");
        job.setJarByClass(GetPatchUpParamsAttribute.class);
        //设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        //设置Map端输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reducer端输出的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置Map端的处理类
        job.setMapperClass(ManagePatchMapper.class);
        //设置Reduce端的处理类
        job.setReducerClass(ManageReducer.class);
        //set the number of reducer
        int reducerNum = Integer.valueOf(otherArgs[2]);
        job.setNumReduceTasks(reducerNum);



        //set the path of cache
        //Attribute File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[6]), job.getConfiguration());
        //Internet_time File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[7]), job.getConfiguration());
        //Online_time File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[8]), job.getConfiguration());
        //Frequence File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[9]), job.getConfiguration());
        //Terminal Types Cache
        DistributedCache.addCacheFile(new URI(otherArgs[10]), job.getConfiguration());
        //dg_info Types Cache
        DistributedCache.addCacheFile(new URI(otherArgs[11]), job.getConfiguration());
        //age_level Cache
        DistributedCache.addCacheFile(new URI(otherArgs[12]), job.getConfiguration());
        //city_level Cache
        DistributedCache.addCacheFile(new URI(otherArgs[13]), job.getConfiguration());


        FileInputFormat.addInputPath(job, new Path(otherArgs[4]));
        FileInputFormat.setMinInputSplitSize(job, Long.parseLong(otherArgs[14]));
        FileInputFormat.setMaxInputSplitSize(job, Long.parseLong(otherArgs[15]));
        //set the hfdf path of output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
