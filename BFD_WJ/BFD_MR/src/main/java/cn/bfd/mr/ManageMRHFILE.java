package cn.bfd.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;


/**
 * Created by yu.fu on 2015/10/15.
 */
public class ManageMRHFILE {

	private static Logger LOG = Logger.getLogger(ManageMRHFILE.class);
    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        //set parameters
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 19){
            System.out.println("Usage: <import_table> <table> <family> <reduceNum> <output> <input> <hfile_input> <region_size> <min_input_split_size> <max_input_split_size> <batch_gids_size> <attribute cache> <internet_time cache> <online_time cache> <frequent cache>"  +
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

        //conf.set("batch_gids_size", otherArgs[8]);
        //hbase configuration
        conf.set("hbase.zookeeper.quorum", "192.168.50.11,192.168.50.12,192.168.50.13,192.168.50.14,192.168.50.15");
		conf.set("zookeeper.znode.parent", "/bfdhbasehot");
		conf.set("hbase.rootdir", "hdfs://bfdhadoop/hbase");
		
		conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily",Integer.valueOf(otherArgs[7]));

        //job configuration
        Job job = Job.getInstance(conf);
		job.setJobName("datamanager_hfile");
		job.setJarByClass(ManageMRHFILE.class);
		
		//String min_input_split_size= otherArgs[6];
		//String max_input_split_size= otherArgs[7];
	/*	Long minInputSplitSize = Long.valueOf(min_input_split_size);
		TextInputFormat.setMinInputSplitSize(job,minInputSplitSize);//设置最小分片大小
		Long maxInputSplitSize = Long.valueOf(max_input_split_size); 
        TextInputFormat.setMaxInputSplitSize(job,maxInputSplitSize);//设置最大分片大小
*/
		
        job.setMapperClass(HFILEMapper.class);
        int reducenum= Integer.valueOf(otherArgs[3]);
        job.setNumReduceTasks(reducenum);
        
        job.setOutputKeyClass(Text.class);
   	 	job.setOutputValueClass(Text.class);
        
        String inputFile = otherArgs[5];
        FileInputFormat.addInputPaths(job, inputFile);
        Path outputPath = new Path(otherArgs[4]);
        FileOutputFormat.setOutputPath(job, outputPath);//输出路径
       //Connection connection = ConnectionFactory.createConnection(conf);
        //TableName tableName = TableName.valueOf(hbase_table_name);
        
        
        HTable import_table = new HTable(conf,import_hbase_table_name.getBytes());
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        
        if (hAdmin.tableExists(import_hbase_table_name)) {
            LOG.info("表"+import_hbase_table_name+"已经存在");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(import_hbase_table_name);
            tableDesc.addFamily(new HColumnDescriptor(hbase_table_family));
            hAdmin.createTable(tableDesc);
            LOG.info("创建表"+import_hbase_table_name+"成功");
            hAdmin.close();
        }
        
        
   	 	
        job.waitForCompletion(true);
        if (job.isSuccessful()){
        	LoadIncrementalHFiles loaders = new LoadIncrementalHFiles(conf);
        	Path hfile_Path = new Path(otherArgs[6]);
        	LOG.info("bulk load begin");
			loaders.doBulkLoad(hfile_Path, import_table);
			LOG.info("bulk load OK");
			System.exit(0);
        } else{
        	System.exit(1);
        }
        
    }
}
