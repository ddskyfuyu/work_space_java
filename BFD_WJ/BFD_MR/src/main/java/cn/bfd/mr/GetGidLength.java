package cn.bfd.mr;

import cn.bfd.protobuf.UserProfile2;
import com.google.protobuf.InvalidProtocolBufferException;
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
import java.util.HashMap;
import java.util.Map;

public class GetGidLength {
	public static class MyMapper extends TableMapper<Text, Text> {

		private String family = new String();
		private String column = new String();
		private Text oupt_key = new Text();
		private Text oupt_value = new Text();
		public Map<String, Integer> filterMap = new HashMap<String, Integer>();

		StringBuilder sb = new StringBuilder();
		public String generateRowKey(String str) {
			char c[] = str.toCharArray();
			char t;
			for (int i = 0; i < (str.length() + 1) / 2; i++) {
				t = c[i];
				c[i] = c[c.length - i - 1];
				c[c.length - i - 1] = t;
			}
			sb = sb.delete(0, sb.length());
			sb.append(c);
			return sb.toString();
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			family = conf.get("FAMILY", "-1");
			column = conf.get("COLUMN", "");

			/*
			Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			String finName = null;
			finName = localFiles[0].toString();
			BufferedReader attr_reader = new BufferedReader(new FileReader(finName));
			String line = "";

			while (((line = attr_reader.readLine()) != null)) {
				if (line.trim().isEmpty() == true) {
					continue;
				}
				String gid = line.trim();
				filterMap.put(gid, 1);
			}
			attr_reader.close();
            */
		}

		public void map(ImmutableBytesWritable row, Result val, Context context)
				throws IOException, InterruptedException {
			if (val.isEmpty()) {
				return;
			}
			String rowKey = Bytes.toString(row.get());
			if (rowKey.isEmpty()) {
				return;
			}

			if (family.equals("-1") || column.equals("-1")) {
				System.out.println("FAMILY, COLUMN key is wrong");
				return;
			}

			/*
			if (!filterMap.containsKey(generateRowKey(rowKey))) {
				return;
			}
            */
			UserProfile2.UserProfile up = null;
			try {
				
				if (val.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))) {
					byte[] up_byte = val.getColumnLatest(Bytes.toBytes(family), Bytes.toBytes(column)).getValue();
					up = UserProfile2.UserProfile.parseFrom(up_byte);
					oupt_key.set(up.getUid());
					oupt_value.set(String.valueOf(up_byte.length));
//					oupt_value.set("1");
					context.write(oupt_key, oupt_value);
				}

			} catch (InvalidProtocolBufferException e) {
				System.out.println("RowKey: " + rowKey);
				return;
			}

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		// set parameters
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 3) {
			System.out.println("<Usage>: table family column output");
			return;
		}
		//conf.set("mapred.textoutputformat.separator", ",");

		// conf.set("mapred.map.tasks", "10");
		// conf.set("mapred.min.split.size", "50000");

		conf.set("Table", otherArgs[0]);
		conf.set("FAMILY", otherArgs[1]);
		conf.set("COLUMN", otherArgs[2]);
		// mini hbase configuration
		conf.set("hbase.zookeeper.quorum", "192.168.112.21,192.168.112.22,192.168.112.23");
		conf.set("zookeeper.znode.parent", "/bfdhbase");
		conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");
		//conf.set("hbase.zookeeper.quorum", "192.168.50.11,192.168.50.12,192.168.50.13,192.168.50.14,192.168.50.15");
		//conf.set("zookeeper.znode.parent", "/bfdhbasehot");
		// conf.set("hbase.rootdir", "hdfs://192.168.96.11:8020/hbase");
		//conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");

		Job job = new Job(conf, "GetGidLength");
		job.setJarByClass(GetGidLength.class);
		// job.setJobName("getUpWithFilter");

		// DEBUG_B
		for (int i = 0; i < otherArgs.length; ++i) {
			System.out.println("The " + i + " argments is " + otherArgs[i]);
		}

		//DistributedCache.addCacheFile(new URI(otherArgs[3]), job.getConfiguration());

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(otherArgs[1]));
		scan.setCaching(1000);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(otherArgs[0]), scan, MyMapper.class, Text.class, Text.class,
				job);
		job.setNumReduceTasks(0);

		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[4]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
