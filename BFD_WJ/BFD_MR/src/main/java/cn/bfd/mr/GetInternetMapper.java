package cn.bfd.mr;

import cn.bfd.protobuf.UserProfile2;
import cn.bfd.tools.GetUserInternetAttributeTmp;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yu.fu on 2015.10.15
 * .
 */
public class GetInternetMapper extends TableMapper<Text, Text> {

    private static Logger LOG = Logger.getLogger(GetInternetMapper.class);
    private String family = null;
    private List<String> prefixList = new ArrayList<String>();
    private List<String> typeList = new ArrayList<String>();
    private Map<String, String> attrMap = new HashMap<String, String>();
    private Map<String, String> internetTimeMap = new HashMap<String, String>();
    private Map<String, String> onlineTimeMap = new HashMap<String, String>();
    private Map<String, String> terminalTypeMap = new HashMap<String, String>();
    private Map<String, String> frequceMap = new HashMap<String, String>();
    private Map<String, String> prefixTypeMap = new HashMap<String, String>();
    private Map<String, Map<String, String>> internetMaps = new HashMap<String, Map<String, String>>();

    private Map<String, String> dgMap = new HashMap<String, String>();
    private Map<String, String> cityMap = new HashMap<String, String>();
    private Map<String, String> ageMap = new HashMap<String, String>();

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text key = new Text();

    private final static String INTNETPREFIX = "/上网特征";


    protected void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        family = conf.get("FAMILY", "-1");
        if("-1".equals(family)){
            System.out.println("The family key is empty.");
            System.exit(1);
        }
        String[] allPrefix = {"/长期购物偏好", "/长期兴趣偏好", "/短期购物偏好", "/短期兴趣偏好", "/当下需求特征", "/潜在需求特征" };
        for(String prefix : allPrefix){
            prefixList.add(prefix);
        }
        String[] allTypes = {"business", "media","business", "media","business","business"};
        for(String type : allTypes){
            typeList.add(type);
        }

        for(int i = 0; i < allPrefix.length; ++i){
            prefixTypeMap.put(allPrefix[i], allTypes[i]);
        }



        //get file from cache, and fill keyMap
        Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String finName = null;

        //fill attribute Map
        finName = localFiles[0].toString();
        BufferedReader attr_reader = new BufferedReader(new FileReader(finName));
        String line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            attrMap.put(colVals[0], colVals[1]);
        }

        //fill Internet_time Map
        finName = localFiles[1].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            internetTimeMap.put(colVals[0], colVals[1]);
            LOG.info("Internet_time map:" + colVals[0] + ","+ colVals[1] );
        }
        internetMaps.put("internet_time", internetTimeMap);

        //fill Online_time Map
        finName = localFiles[2].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            onlineTimeMap.put(colVals[0], colVals[1]);
            LOG.info("Online_time map:" + colVals[0] + "," + colVals[1]);
        }
        internetMaps.put("online_time", onlineTimeMap);

        //fill Frequent Map
        finName = localFiles[3].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            frequceMap.put(colVals[0], colVals[1]);
            LOG.info("frequency map:" + colVals[0] + "," + colVals[1]);
        }
        internetMaps.put("frequency", frequceMap);

        //fill Terminal_Types Map
        finName = localFiles[4].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            LOG.info("Terminal_Types map:" + colVals[0] + "," + colVals[1]);
            terminalTypeMap.put(colVals[0], colVals[1]);
        }
        internetMaps.put("terminal_types", terminalTypeMap);

        //fill dg_info Map
        finName = localFiles[5].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }

            dgMap.put(colVals[0], colVals[1]);
            LOG.info("dgMap: " + colVals[0] + "," + colVals[1]);
        }
        LOG.info("dgMap length: " + dgMap.size());


        //fill age_info Map
        finName = localFiles[6].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            ageMap.put(colVals[0], colVals[1]);
            LOG.info("ageMap: " + colVals[0] + "," + colVals[1]);
        }
        LOG.info("ageMap length: " + ageMap.size());


        //fill city_level Map
        finName = localFiles[7].toString();
        attr_reader = new BufferedReader(new FileReader(finName));
        line = "";
        while (((line = attr_reader.readLine()) != null)) {
            if (line.trim().isEmpty() == true) {
                LOG.warn("The line is empty in " + finName);
                continue;
            }
            String[] colVals = line.trim().split(",");
            if (colVals.length != 2) {
                LOG.warn("The line: " + line + ", the split number is not equal to 2. ");
                continue;
            }
            cityMap.put(colVals[0], colVals[1]);
            LOG.info("cityMap: " + colVals[0] + "," + colVals[1]);
        }
        LOG.info("cityMap length: " + cityMap.size());

    }

    public void map(ImmutableBytesWritable row, Result val, Context context) throws IOException, InterruptedException{

        UserProfile2.UserProfile up = null;
        //Map<String, Integer> resultMap = new HashMap<String, Integer>();
        List<String> resultList = new ArrayList<String>();

        String column = null;

        int up_length = 0;

        if(val.isEmpty()){
            return;
        }
        String rowkey = Bytes.toString(row.get());
        try{
            column = "all";
            if(val.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))){
                byte[] up_byte = val.getColumnLatest(Bytes.toBytes(family), Bytes.toBytes(column)).getValue();
                up_length = up_byte.length;
                up = UserProfile2.UserProfile.parseFrom(up_byte);
            }
            else{
                column = "";
                byte[] up_byte = val.getColumnLatest(Bytes.toBytes(family), Bytes.toBytes(column)).getValue();
                up_length = up_byte.length;
                up = UserProfile2.UserProfile.parseFrom(val.getColumnLatest(Bytes.toBytes(family), Bytes.toBytes(column)).getValue());
            }
        }catch(InvalidProtocolBufferException e){
            LOG.warn("The " + rowkey + " is parseed failed. ");
            //System.out.println("The " + rowkey + " is parseed failed. ");
            return;
        }


        if(up == null){
            LOG.warn("The " + rowkey + " is empty in hbase. ");
            //System.out.println("The " + rowkey + " is empty in hbase. ");
            return;
        }



        //internet features
        if(up.getInterFtsCount() != 0){
            for(int i = 0; i < up.getInterFtsCount(); ++i){
                UserProfile2.InternetFeatures interFt = up.getInterFts(i);
                if(!interFt.hasChanel()){
                    LOG.info("The internet feature does not exist channel attribute. ");
                    continue;
                }
                else{
                    if("Mobile".equals(interFt.getChanel().getValue())){
                        GetUserInternetAttributeTmp.getTerminalBrandsDistribution(interFt, resultList);
                        if(resultList.size() != 0){
                            String brands = StringUtils.join(resultList, ",");
                            word.set(brands + "\t" + up_length);
                            key.set(rowkey);
                            context.write(key, word);
                        }
                    }
                }

            }
        }
//        GetUserInternetAttribute.getAllInternetsDistribution(up, INTNETPREFIX, internetMaps, resultMap);
//        GetUserInternetAttribute.getFirstClass(up, INTNETPREFIX, resultMap);


        

//        for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
//            word.set(entry.getKey());
//            context.write(word, one);
//        }


    }
}
