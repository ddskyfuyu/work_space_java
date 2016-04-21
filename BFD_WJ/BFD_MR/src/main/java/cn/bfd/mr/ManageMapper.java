package cn.bfd.mr;

import cn.bfd.protobuf.UserProfile2;
import cn.bfd.tools.GetMarketingFeaturesAttribute;
import com.google.protobuf.InvalidProtocolBufferException;
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
 * Created by yu.fu on 2015/10/15.
 * 用来完成用户画像指标统计功能，通过使用Map_Reduce的方式
 */
public class ManageMapper extends TableMapper<Text, IntWritable> {

    private static Logger LOG = Logger.getLogger(ManageMapper.class);
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

    private final static String INTNETPREFIX = "/上网特征";


    protected void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        family = conf.get("FAMILY", "-1");
        if("-1".equals(family)){
            System.out.println("The family key is empty.");
            System.exit(1);
        }

        //添加对应的字符串表示以及其对应的用户画像字段，两者之间的关系保存到prefixTypeMap中
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



        //获取在Cache中存储的映射文件路径名
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

        //填充上网时段的映射函数internetTimeMap，将其保存在internetMaps中
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

        //填充上网时长的映射表
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

        //填充上网频次的映射表
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

        //填充终端类型的映射表
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
        Map<String, Integer> resultMap = new HashMap<String, Integer>();
        String column = null;

        if(val.isEmpty()){
            return;
        }
        String rowkey = Bytes.toString(row.get());
        try{
            column = "all";
            if(val.containsColumn(Bytes.toBytes(family), Bytes.toBytes(column))){
                up = UserProfile2.UserProfile.parseFrom(val.getColumnLatest(Bytes.toBytes(family), Bytes.toBytes(column)).getValue());
            }
            else{
                column = "";
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
        resultMap.put("Total", 1);



        //internet features
//        GetUserInternetAttribute.getAllInternetsDistribution(up, INTNETPREFIX, internetMaps, resultMap);
//        GetUserInternetAttribute.getFirstClass(up, INTNETPREFIX, resultMap);

        //dgInfo
//        try{
//            UserProfile2.DemographicInfo dg_info =null;
//
//            if(up.hasDgInfo()) {
//                dg_info = up.getDgInfo();
//                String prefix = "people_data";
//                if (dgMap.containsKey(prefix)) {
//                    prefix = dgMap.get(prefix);
//                }
//                GetDemographicInfoAttribute.getSexDemographicInfoDistribution(dg_info, prefix, dgMap, resultMap);
//                GetDemographicInfoAttribute.getCityDemographicInfoDistribution(dg_info, prefix, dgMap, cityMap, resultMap);
//                GetDemographicInfoAttribute.getAgesDemographicInfoDistribution(dg_info, prefix, dgMap, ageMap, resultMap);
//                GetDemographicInfoAttribute.getBioGenderDemographicInfoDistribution(dg_info, prefix, dgMap, resultMap);
//                GetDemographicInfoAttribute.getBioAgeDemographicInfoDistribution(dg_info, prefix, dgMap, ageMap, resultMap);
//                GetDemographicInfoAttribute.getHasBabyDemographicInfoDistribution(dg_info, prefix, dgMap, resultMap);
//                GetDemographicInfoAttribute.getMarriedDemographicInfoDistribution(dg_info, prefix, dgMap, resultMap);
//                GetDemographicInfoAttribute.getFirstClassDistribution(up, prefix, resultMap);
//                GetDemographicInfoAttribute.getSecondClassDistribution(up, prefix, resultMap);
//            }
//        }catch(Exception e){
//            LOG.info("rowkey: " + rowkey);
//        }


//        for(int i = 0; i < up.getCidInfoCount(); ++i){
//            UserProfile2.CidInfo cidInfo = up.getCidInfo(i);
//            if("Cbaifendian".equals(cidInfo.getCid())){
//                GetUserProfileAttribute.getFirstClass(up, resultMap);
//                for(int j = 0; j < cidInfo.getIndusCount(); ++j){
//                    UserProfile2.IndustryInfo indus = cidInfo.getIndus(j);
//                    for(Map.Entry<String, String> entry : prefixTypeMap.entrySet()) {
//
//                        GetUserProfileAttribute.getFirstCateDistribution(indus, entry.getValue(), entry.getKey(), attrMap, resultMap);
//                        GetUserProfileAttribute.getSecondCateDistribution(indus, entry.getValue(), entry.getKey(), attrMap, resultMap);
//                        GetUserProfileAttribute.getThirdCateDistribution(indus, entry.getValue(), entry.getKey(), attrMap, resultMap);
//                        GetUserProfileAttribute.getFourthCateDistribution(indus, entry.getValue(), entry.getKey(), attrMap, resultMap);
//                        GetUserProfileAttribute.getFifthCateDistribution(indus, entry.getValue(), entry.getKey(), attrMap, resultMap);
//                    }
//                }
//            }
//        }
        
        UserProfile2.MarketingFeatures mf =null;
        //GetMarketingFeaturesAttribute.getFirstClass(up,"/营销特征",resultMap);
        //GetMarketingFeaturesAttribute.getSecondClass(up,"/营销特征",resultMap);
        if(up.hasMarketFt()){
            mf =up.getMarketFt();
            String prefix ="/营销特征/营销";
            GetMarketingFeaturesAttribute.getOldPriceLevelMarketingFeaturesDistribution(mf, prefix, resultMap);
            //GetMarketingFeaturesAttribute.getConCapacityFeaturesDistribution(mf, prefix, dgMap, resultMap);
//	     	   GetMarketingFeaturesAttribute.getConPeriodMarketingFeaturesDistribution(mf, prefix, dgMap, resultMap);
//	     	   GetMarketingFeaturesAttribute.getPriceSensitivityFeaturesDistribution(mf, prefix, dgMap, resultMap);
//	     	   GetMarketingFeaturesAttribute.getPriceLevelMarketingFeaturesDistribution(mf, prefix, dgMap, resultMap);
        }
        

        for(Map.Entry<String, Integer> entry : resultMap.entrySet()){
            word.set(entry.getKey());
            context.write(word, one);
        }


    }
}
