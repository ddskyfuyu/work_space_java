package cn.bfd.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.net.URI;

/**
 * Created by yu.fu on 2015/10/15.
 */
public class GetUpParamsAttributeMain {


    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        //set parameters
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 12){
            System.out.println("Usage: <table> <family> <reduceNum> <output> <attribute cache> <internet_time cache> <online_time cache> <frequent cache>"  +
                                      "<terminal_type cache> <dg_info cache> <age_level cache> <city_level cache> ");
            return;
        }

        conf.set("mapred.textoutputformat.separator", "\t");
        conf.set("Table", otherArgs[0]);
        conf.set("FAMILY", otherArgs[1]);

        //hbase old configuration
        //conf.set("hbase.zookeeper.quorum", "192.168.112.11,192.168.112.12,192.168.112.13");
        //conf.set("zookeeper.znode.parent", "/bfdhbase");
        //conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");

        //conf.set("hbase.zookeeper.quorum", "192.168.50.11,192.168.50.12,192.168.50.13,192.168.50.14,192.168.50.15");
        conf.set("hbase.zookeeper.quorum", "192.168.49.203,192.168.49.204,192.168.49.205");
        conf.set("zookeeper.znode.parent", "/bfdhbasehot");
        conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");


        //job configuration
        Job job = new Job(conf, "GetAttributeMain");
        job.setJarByClass(GetUpParamsAttributeMain.class);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(otherArgs[1]));
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(otherArgs[0]), scan, GetInternetMapper.class, Text.class, Text.class, job);
        job.setReducerClass(GetInternetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the number of reducer
        int reducerNum = Integer.valueOf(otherArgs[2]);
        job.setNumReduceTasks(reducerNum);

        //set the path of cache
        //Attribute File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[4]), job.getConfiguration());
        //Internet_time File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[5]), job.getConfiguration());
        //Online_time File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[6]), job.getConfiguration());
        //Frequence File Cache
        DistributedCache.addCacheFile(new URI(otherArgs[7]), job.getConfiguration());
        //Terminal Types Cache
        DistributedCache.addCacheFile(new URI(otherArgs[8]), job.getConfiguration());
        //dg_info Types Cache
        DistributedCache.addCacheFile(new URI(otherArgs[9]), job.getConfiguration());
        //age_level Cache
        DistributedCache.addCacheFile(new URI(otherArgs[10]), job.getConfiguration());
        //city_level Cache
        DistributedCache.addCacheFile(new URI(otherArgs[11]), job.getConfiguration());



        //set the hfdf path of output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
