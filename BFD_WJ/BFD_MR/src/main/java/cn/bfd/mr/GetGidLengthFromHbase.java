package cn.bfd.mr;

/**
 * 本程序主要用来针对用户画像的各个指标维度进行统计
 * 使用MR程序，统计各个指标的分布，输出到指定的HDFS目录下
 * author: yu.fu
 * date: 2015-12-25
 * phone: 15120071966
 */
import cn.bfd.data_bank.GetAttrDistributionMapper;
import cn.bfd.tools.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.Map;

public class GetGidLengthFromHbase extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		if (args.length < 7) {
			System.out.println("<Usage> config_file hbase_table family column scan_size inputPath outputPath");
			System.exit(-1);
		}
		int result = ToolRunner.run(new GetGidLengthFromHbase(), args);
		System.exit(result);
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		
		FileSystem fs = FileSystem.get(configuration); 

        String config_file = args[0];
        String hbase_table = args[1];
        String family = args[2];
        String columns = args[3];
		String scan_gid_size = args[4];
        String inputPath = args[5];
        String outputPath = args[6];


		Map<String,String> map = new HashMap<String,String>();
		FileUtils.FillStrValMap(map, config_file, "=", fs);

		//配置输出文件中列与列之间的分隔符
		configuration.set("mapred.textoutputformat.separator", ",");


	    // 配置hbase thrift ip
		if(map.containsKey("hbase_thrift_ip")){
			configuration.set("hbase_thrift_ip", map.get("hbase_thrift_ip"));
		}
		else{
			System.out.println("The hbase_thrift_ip does not exist in map. ");
			System.exit(-1);
		}

		// 配置hbase thrift port
		if(map.containsKey("hbase_thrift_port")){
			configuration.set("hbase_thrift_port", map.get("hbase_thrift_port"));
		}
		else{
			System.out.println("The hbase_thrift_port does not exist in map. ");
			System.exit(-1);
		}

		// 配置hbase列名字符串
		if(columns != null){
			configuration.set("column",columns);
		}
		else{
			System.out.println("The columns is null. ");
			System.exit(-1);
		}

		// 配置hbase的列簇名称
		if(family != null){
			configuration.set("family",family);
		}
		else{
			System.out.println("The family is null. ");
			System.exit(-1);
		}

		// 配置hbase表名
		if(hbase_table != null){
			configuration.set("hbase_table",hbase_table);
		}
		else{
			System.out.println("The table is null. ");
			System.exit(-1);
		}

		// 配置批处理的gid的个数
		if((scan_gid_size != null) && (Integer.valueOf(scan_gid_size) > 0)){
			configuration.set("scan_gid_size",scan_gid_size);
		}
		else{
			System.out.println("The scan_gid_size is wrong. ");
			System.exit(-1);
		}

		//
		
        Job job = Job.getInstance(configuration, "GetGidLengthFromHbase");
        job.setJarByClass(GetGidLengthFromHbase.class);

		job.setMapperClass(GetAttrDistributionMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		if (args.length>7) {
			String min_split=args[7];
			System.out.println("MinSplitSize before set:"+FileInputFormat.getMinSplitSize(job));
			FileInputFormat.setMinInputSplitSize(job, Long.parseLong(min_split));
			System.out.println("MinSplitSize after set:"+FileInputFormat.getMinSplitSize(job));
		}
		if (args.length>8) {
			String max_split=args[8];
			System.out.println("MaxSplitSize before set:"+FileInputFormat.getMaxSplitSize(job));
			FileInputFormat.setMaxInputSplitSize(job, Long.parseLong(max_split));
			System.out.println("MaxSplitSize after set:"+FileInputFormat.getMaxSplitSize(job));
		}
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
