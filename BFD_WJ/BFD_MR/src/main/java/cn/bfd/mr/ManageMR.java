package cn.bfd.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


/**
 * Created by BFD_278 on 2015/10/15.
 */
public class ManageMR {

	private static Logger LOG = Logger.getLogger(ManageMR.class);
    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        //set parameters
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 17){
            System.out.println("Usage: <import_table> <table> <family> <reduceNum> <output> <input> <min_input_split_size> <max_input_split_size> <batch_gids_size> <attribute cache> <internet_time cache> <online_time cache> <frequent cache>"  +
                                      "<terminal_type cache> <dg_info cache> <age_level cache> <city_level cache> ");
            return;
        }

        String import_hbase_table_name = otherArgs[0];
        String hbase_table_name = otherArgs[1];
        String hbase_table_family= otherArgs[2];
        //conf.set("mapred.textoutputformat.separator", ",");
        conf.set("hbase_table_name", otherArgs[1]);
        conf.set("import_hbase_table_name", otherArgs[0]);
        conf.set("hbase_table_family", otherArgs[2]);

        conf.set("batch_gids_size", otherArgs[8]);
        //hbase configuration
        conf.set("hbase.zookeeper.quorum", "192.168.50.11,192.168.50.12,192.168.50.13,192.168.50.14,192.168.50.15");
		conf.set("zookeeper.znode.parent", "/bfdhbasehot");
		conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");

        //job configuration
        Job job = Job.getInstance(conf);
		job.setJobName("datamanager_hbase2hbase");
		job.setJarByClass(ManageMR.class);
		
		String min_input_split_size= otherArgs[6];
		String max_input_split_size= otherArgs[7];
		Long minInputSplitSize = Long.valueOf(min_input_split_size);
		TextInputFormat.setMinInputSplitSize(job,minInputSplitSize);//设置最小分片大小
		Long maxInputSplitSize = Long.valueOf(max_input_split_size); 
        TextInputFormat.setMaxInputSplitSize(job,maxInputSplitSize);//设置最大分片大小

		
        job.setMapperClass(ManageMapper.class);
        int reducenum= Integer.valueOf(otherArgs[3]);
        job.setNumReduceTasks(reducenum);
        //job.setReducerClass(KeyValueSortReducer.class);
        //job.setReducerClass(ManageReducer.class);
        //job.setReducerClass(cls);
        
        
        job.setOutputKeyClass(ImmutableBytesWritable.class);
   	 	job.setOutputValueClass(KeyValue.class);
        
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(otherArgs[4]);
        
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);//如果输出路径存在，就将其删除
        }
        //String inputFile = "hdfs://bfdhadoop26/user/bre/yu.fu/data_manage_test/test_gids.txt";
        String inputFile = otherArgs[5];
        FileInputFormat.addInputPaths(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputPath);//输出路径
        //Connection connection = ConnectionFactory.createConnection(conf);
        //TableName tableName = TableName.valueOf(hbase_table_name);
        
        
        HTable import_table = new HTable(conf,import_hbase_table_name.getBytes());
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        
        if (hAdmin.tableExists(import_hbase_table_name)) {
            LOG.info("表已经存在");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(import_hbase_table_name);
            tableDesc.addFamily(new HColumnDescriptor(hbase_table_family));
            hAdmin.createTable(tableDesc);
            LOG.info("创建表成/user/bre/yu.fu/data_manage_test功");
            hAdmin.close();
        }
        
   	 	//Table hbase_table = connection.getTable(tableName);
   	 	//RegionLocator regionLocator =  connection.getRegionLocator(tableName);
        //HFileOutputFormat2.configureIncrementalLoad(job, hbase_table, regionLocator);

        //HFileOutputFormat2.configureIncrementalLoad(job, import_table);
   	 	
        job.waitForCompletion(true);
        if (job.isSuccessful()){
        	//LoadIncrementalHFiles loaders = new LoadIncrementalHFiles(conf);
			//loaders.doBulkLoad(outputPath, import_table);
			System.exit(0);
        } else{
        	System.exit(1);
        }
        
    }
}
