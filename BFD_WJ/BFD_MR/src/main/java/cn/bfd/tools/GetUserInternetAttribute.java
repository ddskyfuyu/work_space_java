package cn.bfd.tools;

import cn.bfd.protobuf.PortraitClass;
import cn.bfd.protobuf.UserProfile2;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yu.fu on 2015/10/8.
 */
public class GetUserInternetAttribute {

    private static Logger LOG = Logger.getLogger(GetUserInternetAttribute.class);


    public static void getFirstClass(UserProfile2.UserProfile up,
                                     String prefix,
                                     Map<String, Integer> result){
        if(up.getInterFtsCount() != 0){
            result.put(prefix, 1);
        }
    }

    public static void getFirstClass(Map<String, Object> objMap,
                                     String prefix,
                                     Map<String, Integer> result){
        if(objMap.containsKey("inet_PC") || objMap.containsKey("inet_Mobile")){
            result.put(prefix, 1);
        }
    }


    /*
     *  get the Distribution of Online time Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
     */
    public static void getOnlineTimeDistribution(UserProfile2.InternetFeatures internetFts,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }

        //set the prefix string to the first position of the key_list
        String attr_name = "上网时长";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);

        for(int i = 0; i < internetFts.getOnlineTimeCount(); ++i){
            UserProfile2.SInfo online_time = internetFts.getOnlineTime(i);

//            if(!keyMap.containsKey(online_time.getValue())){
//                LOG.info("online_time:" + online_time.getValue());
//                continue;
//            }
//            String value = keyMap.get(online_time.getValue());
            String value = online_time.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of Online time Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getOnlineTimeDistribution(PortraitClass.InternetFeatures internetFts,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }

        //set the prefix string to the first position of the key_list
        String attr_name = "上网时长";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);

        if(internetFts.hasOnlineTime()){

            PortraitClass.SInfo online_time = internetFts.getOnlineTime();
            String value = online_time.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of internet_time Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getInternetTimeDistribution(UserProfile2.InternetFeatures internetFts,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){


        //get the distribution of Internet Time for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }

        //set the prefix string to the first position of the key_list
        String attr_name = "上网时段";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getInternetTimeCount(); ++i){
            UserProfile2.SInfo internet_time = internetFts.getInternetTime(i);
//            LOG.info("internet_time real :" + internet_time.getValue());
            if(!keyMap.containsKey(internet_time.getValue())){
                LOG.info("internet_time:" + internet_time.getValue());
                continue;
            }
            String value = keyMap.get(internet_time.getValue());
            //String value = internet_time.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    public static void getInternetTimeDistribution(PortraitClass.InternetFeatures internetFts,
                                                   String type,
                                                   String prefix,
                                                   Map<String, String> keyMap,
                                                   Map<String, Integer> result){


        //get the distribution of Internet Time for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }

        //set the prefix string to the first position of the key_list
        String attr_name = "上网时段";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getInternetTimeCount(); ++i){
            PortraitClass.SInfo internet_time = internetFts.getInternetTime(i);
//            LOG.info("internet_time real :" + internet_time.getValue());
            if(!keyMap.containsKey(internet_time.getValue())){
                LOG.info("internet_time:" + internet_time.getValue());
                continue;
            }
            String value = keyMap.get(internet_time.getValue());
            //String value = internet_time.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of Frequency Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
     */
    public static void getFrequeceDistribution(UserProfile2.InternetFeatures internetFts,
                                                  String type,
                                                  String prefix,
                                                  Map<String, String> keyMap,
                                                  Map<String, Integer> result){


        //get the distribution of Frequence for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "上网频次";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getFrequencyCount(); ++i){
            UserProfile2.SInfo frequency = internetFts.getFrequency(i);
            if(!keyMap.containsKey(frequency.getValue())){
                //LOG.info("frequency:" + frequency.getValue());
                continue;
            }
            String value = keyMap.get(frequency.getValue());
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    public static void getFrequeceDistribution(PortraitClass.InternetFeatures internetFts,
                                               String type,
                                               String prefix,
                                               Map<String, String> keyMap,
                                               Map<String, Integer> result){


        //get the distribution of Frequence for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "上网频次";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        if(internetFts.hasFrequency()){
            PortraitClass.SInfo frequency = internetFts.getFrequency();
            if(!keyMap.containsKey(frequency.getValue())){
                return;
            }
            String value = keyMap.get(frequency.getValue());
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of Browser Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
     */
    public static void getBrowserDistribution(UserProfile2.InternetFeatures internetFts,
                                               String type,
                                               String prefix,
                                               Map<String, Integer> result){


        //get the distribution of Browser for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "浏览器";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getBrowserCount(); ++i){
            UserProfile2.SInfo browser = internetFts.getBrowser(i);
            String value = browser.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    public static void getBrowserDistribution(PortraitClass.InternetFeatures internetFts,
                                              String type,
                                              String prefix,
                                              Map<String, Integer> result){


        //get the distribution of Browser for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "浏览器";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getBrowserCount(); ++i){
            PortraitClass.SInfo browser = internetFts.getBrowser(i);
            String value = browser.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of oper_systems Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getOperSystemsDistribution(UserProfile2.InternetFeatures internetFts,
                                                  String type,
                                                  String prefix,
                                                  Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }


        //set the prefix string to the first position of the key_list
        String attr_name = "操作系统";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getOperSystemsCount(); ++i){
            UserProfile2.SInfo oper_sys = internetFts.getOperSystems(i);
            String value = oper_sys.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    public static void getOperSystemsDistribution(PortraitClass.InternetFeatures internetFts,
                                                  String type,
                                                  String prefix,
                                                  Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }


        //set the prefix string to the first position of the key_list
        String attr_name = "操作系统";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getOperSystemsCount(); ++i){
            PortraitClass.SInfo oper_sys = internetFts.getOperSystems(i);
            String value = oper_sys.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }


    /*
     *  get the Distribution of oper_systems Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getTerminalTypesDistribution(UserProfile2.InternetFeatures internetFts,
                                                    String type,
                                                    String prefix,
                                                    Map<String, String> keyMap,
                                                    Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "终端类型";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getTerminalTypesCount(); ++i){
            UserProfile2.SInfo terminal_type = internetFts.getTerminalTypes(i);
//            if(!keyMap.containsKey(terminal_type.getValue())) {
//                LOG.info("terminal_type:" + terminal_type.getValue());
//                continue;
//            }
            //String value = keyMap.get(terminal_type.getValue());
            String value = terminal_type.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    public static void getTerminalTypesDistribution(PortraitClass.InternetFeatures internetFts,
                                                    String type,
                                                    String prefix,
                                                    Map<String, String> keyMap,
                                                    Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "终端类型";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);
        for(int i = 0; i < internetFts.getTerminalTypesCount(); ++i){
            PortraitClass.SInfo terminal_type = internetFts.getTerminalTypes(i);
//            if(!keyMap.containsKey(terminal_type.getValue())) {
//                LOG.info("terminal_type:" + terminal_type.getValue());
//                continue;
//            }
            //String value = keyMap.get(terminal_type.getValue());
            String value = terminal_type.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of terminal brands Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getTerminalBrandsDistribution(UserProfile2.InternetFeatures internetFts,
                                                    String type,
                                                    String prefix,
                                                    Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "终端品牌";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);

        for(int i = 0; i < internetFts.getTerminalBrandsCount(); ++i){
            UserProfile2.SInfo terminal_brand = internetFts.getTerminalBrands(i);
            String value = terminal_brand.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    public static void getTerminalBrandsDistribution(PortraitClass.InternetFeatures internetFts,
                                                     String type,
                                                     String prefix,
                                                     Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "终端品牌";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);

        for(int i = 0; i < internetFts.getTerminalBrandsCount(); ++i){
            PortraitClass.SInfo terminal_brand = internetFts.getTerminalBrands(i);
            String value = terminal_brand.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }



    /*
     *  get the Distribution of Access Ways Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getAccessWaysDistribution(UserProfile2.InternetFeatures internetFts,
                                                 String type,
                                                 String prefix,
                                                 Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        String attr_name = "联网方式";
        key_list.add(prefix);
        key_list.add(channel);
        key_list.add(attr_name);

        for(int i = 0; i < internetFts.getAccessWayCount(); ++i){
            UserProfile2.SInfo acess_way = internetFts.getAccessWay(i);
            String value = acess_way.getValue();
            //judge whether the key_list contains the second element
            if(key_list.size() < 4){
                key_list.add(value);
            }
            else{
                key_list.set(3, value);
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);
        }
    }

    /*
     *  get the Distribution of Access Ways Internet Attribute
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
    */
    public static void getChannelsDistribution(UserProfile2.InternetFeatures internetFts,
                                               String type,
                                               String prefix,
                                               Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        key_list.add(prefix);
        key_list.add(channel);
        String key = StringUtils.join(key_list, "/");
        result.put(key, 1);

    }

    public static void getChannelsDistribution(PortraitClass.InternetFeatures internetFts,
                                               String type,
                                               String prefix,
                                               Map<String, Integer> result){


        //get the distribution of OnlineTime for PC channel
        List<String> key_list = new ArrayList<String>();
        String channel = null;
        if("PC".equals(type)){
            channel = "PC";
        }
        else if("Mobile".equals(type)){
            channel = "移动端";
        }
        else{
            LOG.info("The channel type is wrong. " + type);
            return;
        }
        //set the prefix string to the first position of the key_list
        key_list.add(prefix);
        key_list.add(channel);
        String key = StringUtils.join(key_list, "/");
        result.put(key, 1);

    }


    /*
     *  get the Distribution of the Second Category for long preferences
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, save the results of the distribution of the all categories
     *  @param: prefix, the prefix string of the second category(长期购物偏好和长期兴趣偏好)
     *  @return: void
     */
    public static void getAllInternetsDistribution(UserProfile2.UserProfile up,
                                                   String prefix,
                                                   Map<String, Map<String, String>> keyMaps,
                                                   Map<String, Integer> result){

        for(int i = 0; i < up.getInterFtsCount(); ++i){
            UserProfile2.InternetFeatures interFt = up.getInterFts(i);
            if(!interFt.hasChanel()){
                LOG.warn("The internet feature does not exist channel attribute. ");
                continue;
            }
            else{
                String key = null;
                if("PC".equals(interFt.getChanel().getValue())){
                    //get the distribution of the online_time attribute
                    key = "online_time";
                    if(keyMaps.containsKey(key)){
                        getOnlineTimeDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the frequency attribute
                    key = "frequency";
                    if(keyMaps.containsKey(key)){
                        getFrequeceDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the frequency attribute
                    key = "internet_time";
                    if(keyMaps.containsKey(key)){
                        getInternetTimeDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the browser attribute;
                    getBrowserDistribution(interFt, "PC", prefix, result);

                    //get the distribution of the oper_systems attribute
                    getOperSystemsDistribution(interFt, "PC", prefix, result);

                    //get the distribution of the terminal_types attribute
                    key = "terminal_types";
                    if(keyMaps.containsKey(key)){
                        getTerminalTypesDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the terminal_brands attribute
                    getTerminalBrandsDistribution(interFt, "PC", prefix, result);

                    //get the distribution of the channel attribute
                    getChannelsDistribution(interFt, "PC", prefix, result);
                }
                else if("Mobile".equals(interFt.getChanel().getValue())){

                    //get the distribution of the online_time attribute
                    key = "online_time";
                    if(keyMaps.containsKey(key)){
                        getOnlineTimeDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the frequency attribute
                    key = "frequency";
                    if(keyMaps.containsKey(key)){
                        getFrequeceDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the frequency attribute
                    key = "internet_time";
                    if(keyMaps.containsKey(key)){
                        getInternetTimeDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the browser attribute
                    getBrowserDistribution(interFt, "Mobile", prefix, result);

                    //get the distribution of the oper_systems attribute
                    getOperSystemsDistribution(interFt, "Mobile", prefix, result);

                    //get the distribution of the terminal_types attribute
                    key = "terminal_types";
                    if(keyMaps.containsKey(key)){
                        getTerminalTypesDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
                    }

                    //get the distribution of the terminal_brands attribute
                    getTerminalBrandsDistribution(interFt, "Mobile", prefix, result);

                    //get the distribution of the channel attribute
                    getChannelsDistribution(interFt, "Mobile", prefix, result);

                    //get the distribution of the Access Channel
                    getAccessWaysDistribution(interFt, "Mobile", prefix, result);
                }
            }
        }
    }


    public static void getPCInternetsDistribution(Object obj,
                                                   String prefix,
                                                   Map<String, Map<String, String>> keyMaps,
                                                   Map<String, Integer> result){

        if(obj == null){
            LOG.error("obj for getPCInternetsDistribution is null");
            return;
        }
        PortraitClass.InternetFeatures interFt = (PortraitClass.InternetFeatures)obj;
        String key = null;

        //get the distribution of the online_time attribute
        key = "online_time";
        if(keyMaps.containsKey(key)){
            getOnlineTimeDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the frequency attribute
        key = "frequency";
        if(keyMaps.containsKey(key)){
            getFrequeceDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the frequency attribute
        key = "internet_time";
        if(keyMaps.containsKey(key)){
            getInternetTimeDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the browser attribute;
        getBrowserDistribution(interFt, "PC", prefix, result);

        //get the distribution of the oper_systems attribute
        getOperSystemsDistribution(interFt, "PC", prefix, result);

        //get the distribution of the terminal_types attribute
        key = "terminal_types";
        if(keyMaps.containsKey(key)){
            getTerminalTypesDistribution(interFt, "PC", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the terminal_brands attribute
        getTerminalBrandsDistribution(interFt, "PC", prefix, result);

        //get the distribution of the channel attribute
        getChannelsDistribution(interFt, "PC", prefix, result);
    }

    public static void getMobileInternetsDistribution(Object obj,
                                                      String prefix,
                                                      Map<String, Map<String, String>> keyMaps,
                                                      Map<String, Integer> result){

        if(obj == null){
            LOG.error("obj for getPCInternetsDistribution is null");
            return;
        }
        PortraitClass.InternetFeatures interFt = (PortraitClass.InternetFeatures)obj;
        String key = null;

        //get the distribution of the online_time attribute
        key = "online_time";
        if(keyMaps.containsKey(key)){
            getOnlineTimeDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the frequency attribute
        key = "frequency";
        if(keyMaps.containsKey(key)){
            getFrequeceDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the frequency attribute
        key = "internet_time";
        if(keyMaps.containsKey(key)){
            getInternetTimeDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the browser attribute;
        getBrowserDistribution(interFt, "Mobile", prefix, result);

        //get the distribution of the oper_systems attribute
        getOperSystemsDistribution(interFt, "Mobile", prefix, result);

        //get the distribution of the terminal_types attribute
        key = "terminal_types";
        if(keyMaps.containsKey(key)){
            getTerminalTypesDistribution(interFt, "Mobile", prefix, keyMaps.get(key), result);
        }

        //get the distribution of the terminal_brands attribute
        getTerminalBrandsDistribution(interFt, "Mobile", prefix, result);

        //get the distribution of the channel attribute
        getChannelsDistribution(interFt, "Mobile", prefix, result);
    }





}
