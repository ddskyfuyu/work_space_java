package cn.bfd.es;

import cn.bfd.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.HashMap;
import java.util.Map;

public class ES_DB extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		if (args.length < 7 ) {
			System.out.println("<Usage> config_file es_index es_type scan_size inputPath outputPath column_size");
			System.exit(-1);
		}
		int result = ToolRunner.run(new ES_DB(), args);
		System.exit(result);
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();
		
		FileSystem fs = FileSystem.get(configuration); 

        String config_file = args[0];
		String es_index = args[1];
		String es_type = args[2];
		String scan_gid_size = args[3];
        String inputPath = args[4];
        String outputPath = args[5];
		String column_size = args[6];


		
		Map<String,String> map = new HashMap<String,String>();
		FileUtils.FillStrValMap(map, config_file, "=", fs);

		//设置elasticsearch配置
        configuration.set("cluster_name", map.get("cluster_name"));
        configuration.set("es_ip", map.get("es_ip"));
        configuration.set("es_port", map.get("es_port"));
        configuration.set("es_index", es_index);
        configuration.set("es_type", es_type);

		//设置批量处理个数
        configuration.set("scan_gid_size", scan_gid_size);

		//设置处理列个数
		configuration.set("column_size", column_size);

		//MR任务配置
        Job job = Job.getInstance(configuration, "databank_upES");
        job.setJarByClass(ES_DB.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(ES_DB_Mapper.class);


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
		job.setNumReduceTasks(0);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
