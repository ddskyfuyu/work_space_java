package cn.bfd.mr;

import cn.bfd.tools.CommonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yu.fu on 2015/10/15.
 */
public class ManageMapper1 extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    private static Logger LOG = Logger.getLogger(ManageMapper.class);
    private List<String> gids = new ArrayList<String>();
    private int batch_gids_size = 0;//批处理从hbase中取数据条数
    
    private Table hbase_table = null;
    private Table import_hbase_table = null;
    private String hbase_table_name = null;
    private String import_hbase_table_name = null;
    private String hbase_table_family = null;
    private String hbase_table_column = "all";
    
    protected void setup(Context context) throws IOException, InterruptedException{
    	//Configuration conf = HBaseConfiguration.create();
        Configuration conf = context.getConfiguration();
    	batch_gids_size = Integer.valueOf(conf.get("batch_gids_size","100"));
    	hbase_table_name = conf.get("hbase_table_name");
    	import_hbase_table_name = conf.get("import_hbase_table_name");
    	hbase_table_family = conf.get("hbase_table_family","-1");
    	hbase_table_column = conf.get("hbase_table_column","all");
    	
   	 	Connection connection = ConnectionFactory.createConnection(conf);
   	 	hbase_table = connection.getTable(TableName.valueOf(hbase_table_name));
   	    import_hbase_table = connection.getTable(TableName.valueOf(import_hbase_table_name));
       
        if("-1".equals(hbase_table_family) ){
            LOG.error("The family key is empty.");
            System.exit(1);
        }
        
        LOG.info("11-----"+batch_gids_size);
        LOG.info("12-----"+hbase_table_name);
        LOG.info("13-----"+hbase_table_family);
        LOG.info("14-----"+hbase_table_column);

    }

    public List<Get> getHBaseGets(List<String> gids){
    	
    	List<Get> gets = new ArrayList<Get>();
    	if (gids != null) {
    		for (String gid : gids) {
    			LOG.info("20-----" + gid);
				LOG.info("21-----" + batch_gids_size);
				LOG.info("22-----" + hbase_table_name);
				LOG.info("23-----" + hbase_table_family);
				LOG.info("24-----" + hbase_table_column);
    			String rowKey = CommonUtils.reverseString(gid);
    			Get get = new Get(Bytes.toBytes(rowKey));  
    			get.addFamily(Bytes.toBytes(hbase_table_family));
    			//get.addColumn(Bytes.toBytes(hbase_table_family), Bytes.toBytes(hbase_table_column));
    			gets.add(get);
    		}
		}
    	
    	return gets;
    	
    }
    
    public void wrapHBasePutAndWrite(List<Get> gets,Context context){
    	
    	if (gets != null && (gets.size() > 0)) {
    		LOG.info("gets size-----" + gets.size());
    		List<Put> puts = new ArrayList<Put>();
			try {
				Result[] results = hbase_table.get(gets);
				LOG.info("res size-----" + results.length);
				for (Result result : results) {
					if (result != null) {
							
						boolean column_exists = result.containsColumn(hbase_table_family.getBytes(), hbase_table_column.getBytes());
						String row_key = null;
						byte[] value = null;
						try {
							if (column_exists) {
								//Cell cell = result.getColumnLatestCell(Bytes.toBytes(hbase_table_family), Bytes.toBytes(hbase_table_column));
								row_key = Bytes.toString(result.getRow());
								value = result.getColumnLatest(Bytes.toBytes(hbase_table_family), Bytes.toBytes(hbase_table_column)).getValue();
								//value =  Bytes.toString(cell.getValueArray());
							}else{
								//Cell cell = result.getColumnLatestCell(Bytes.toBytes(hbase_table_family), Bytes.toBytes(""));
								try {
									row_key = Bytes.toString(result.getRow());
									value = result.getColumnLatest(Bytes.toBytes(hbase_table_family), Bytes.toBytes("")).getValue();
								} catch (Exception e) {
									LOG.info("error_empty_last_row_key--------"+row_key);
									LOG.info("result is--"+result.getColumnLatest(Bytes.toBytes(hbase_table_family), Bytes.toBytes("")));
								}
								//value =  Bytes.toString(cell.getValueArray());
							}
							
							if (row_key!=null && value!=null) {
								LOG.info("column exists--------"+column_exists);
								LOG.info("row_key--------"+row_key);
								LOG.info("value--------"+Bytes.toString(value));
								//ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(row_key));
								//KeyValue rowLine = new KeyValue(Bytes.toBytes(row_key),Bytes.toBytes(hbase_table_family), Bytes.toBytes(hbase_table_column), value);
								Put put = new Put(Bytes.toBytes(row_key));
								put.add(Bytes.toBytes(hbase_table_family), Bytes.toBytes(hbase_table_column), value);
						        puts.add(put);
								
								//context.write(rowKey, rowLine);
							}
						} catch (Exception e) {
							LOG.info("error----------row_key------"+row_key);
							e.printStackTrace();
						}
					}
					
				}
				
				if(puts != null && puts.size() > 0){
					LOG.info("put----------"+puts.size()+"------"+import_hbase_table_name);
					import_hbase_table.put(puts);
					import_hbase_table.flushCommits();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		
			
		}
    }
    
    
    
    @Override
    protected void map(LongWritable key, Text val, Context context)
    		throws IOException, InterruptedException {

    	if (val != null && val.toString().length() > 0) {
			
    		String gid = val.toString();
    		gids.add(gid);
    		if (gids.size() == batch_gids_size) {
    			LOG.info("1-----batch---"+gids.size());
    			List<Get> gets = getHBaseGets(gids);
    			wrapHBasePutAndWrite(gets, context);
    			gids.clear();
			}
    		
		}
    
    }
    
    @Override
    protected void cleanup(Context context)
    		 {
    	if (gids.size() > 0) {
    		LOG.info("2-----batch---"+gids.size());
    		List<Get> gets = getHBaseGets(gids);
			wrapHBasePutAndWrite(gets, context);
		}
    	try {
    		
	    	if (hbase_table != null) {
					hbase_table.close();
			}
	    	
	    	if (import_hbase_table != null) {
				import_hbase_table.close();
			}
    	} catch (Exception e) {
			LOG.info("hbase table close error");
			e.printStackTrace();
		}
    	
    }
}
