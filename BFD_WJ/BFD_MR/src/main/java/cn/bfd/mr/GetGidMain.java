package cn.bfd.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class GetGidMain {
	public static class MyMapper extends TableMapper<Text, Text> {
		private Text output_key = new Text();
		private Text output_val = new Text();

		public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException {
			if (val.isEmpty()) {
				return;
			}
			String rowKey = Bytes.toString(row.get());
			if (rowKey.isEmpty() || rowKey == null) {
				return;
			}
			output_key.set(rowKey);
			output_val.set("");
			context.write(output_key, output_val);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		// set parameters
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.out.println("<Usage> table family output");
			return;
		}

		// hbase configuration
		conf.set("hbase.zookeeper.quorum", "192.168.49.203,192.168.49.204,192.168.49.205");
		conf.set("zookeeper.znode.parent", "/bfdhbasehot");
		conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");

		//job configuration
		Job job = new Job(conf, "GetRowKey");
		job.setJarByClass(GetGidMain.class);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(otherArgs[1]));
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(otherArgs[0]), scan, MyMapper.class, Text.class, Text.class, job);

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
