package cn.bfd.tools;

import cn.bfd.protobuf.PortraitClass;
import cn.bfd.protobuf.UserProfile2;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by yu.fu on 2015/10/8.
 */
public class GetUserProfileAttribute {

    private static Logger LOG = Logger.getLogger(GetMarketingFeaturesAttribute.class);


    public static void getFirstClass(UserProfile2.UserProfile up,
                                     Map<String, Integer> result){

        for(int i = 0; i < up.getCidInfoCount(); ++i){
            UserProfile2.CidInfo cidInfo = up.getCidInfo(i);
            if("Cbaifendian".equals(cidInfo.getCid())){
                for(int j = 0; j < cidInfo.getIndusCount(); ++j){
                    UserProfile2.IndustryInfo indus = cidInfo.getIndus(j);
                    if(indus.getFirstCateCount() != 0){
                        result.put("/长期购物偏好", 1);
                    }
                    if(indus.getMediaCateCount() != 0){
                        result.put("/长期兴趣偏好", 1);
                    }
                }
            }
        }
    }

    /*
     * get the Distribution of all of attributes of the Attribution of the category
     * @param: prefix, string, the prefix string of key
     * @param: keyMap, map, the relation of attribute of the attributeInfo and key string
     * @param: result, map, save the final result, key = prefix + "/" + key, value = 1
     * @return: void
     */
    public static void getAllOfAttributionDistribution(UserProfile2.AttributeInfo attributeInfo,
                                                       String prefix,
                                                       Map<String, String> keyMap,
                                                       Map<String, Integer> result){
        //get the distribution of cos_sunblock
        for(int i = 0; i < attributeInfo.getCosSunblockCount(); ++i){
            String attr_key = "cos_sunblock";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);

            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getCosSunblock(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getCosSunblock(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of departures
        for(int i = 0; i < attributeInfo.getDeparturesCount(); ++i){
            String attr_key = "departures";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getDepartures(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getDepartures(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of destination
        for(int i = 0; i < attributeInfo.getDestinationCount(); ++i){
            String attr_key = "destination";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getDestination(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getDestination(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of destination
        for(int i = 0; i < attributeInfo.getDestinationCount(); ++i){
            String attr_key = "destination";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getDestination(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getDestination(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cmp_video
        for(int i = 0; i < attributeInfo.getCmpVideoCount(); ++i){
            String attr_key = "cmp_video";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getCmpVideo(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getCmpVideo(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_currency
        for(int i = 0; i < attributeInfo.getFinaCurrencyCount(); ++i){
            String attr_key = "fina_currency";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
           // key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getFinaCurrency(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getFinaCurrency(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of fina_currency
        for(int i = 0; i < attributeInfo.getQcEdModelCount(); ++i){
            String attr_key = "qc_ed_model";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getQcEdModel(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getQcEdModel(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_ed_model
        for(int i = 0; i < attributeInfo.getQcEdModelCount(); ++i){
            String attr_key = "qc_ed_model";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getQcEdModel(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getQcEdModel(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fashion
        for(int i = 0; i < attributeInfo.getFashionCount(); ++i){
            String attr_key = "fashion";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(attributeInfo.getFashion(i).getValue());
                }
                else{
                    key_list.set(2, attributeInfo.getFashion(i).getValue());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of durations
        for(int i = 0; i < attributeInfo.getDurationsCount(); ++i){
            String attr_key = "durations";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getDurations(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getDurations(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cos_situation
        for(int i = 0; i < attributeInfo.getCosSituationCount(); ++i){
            String attr_key = "cos_situation";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCosSituation(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCosSituation(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of clo_sleeve
        for(int i = 0; i < attributeInfo.getCloSleeveCount(); ++i){
            String attr_key = "clo_sleeve";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCloSleeve(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCloSleeve(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mdc_symptom
        for(int i = 0; i < attributeInfo.getMdcSymptomCount(); ++i){
            String attr_key = "mdc_symptom";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMdcSymptom(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMdcSymptom(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_year
        for(int i = 0; i < attributeInfo.getQcYearCount(); ++i){
            String attr_key = "qc_year";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcYear(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcYear(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cmp_cpu
        for(int i = 0; i < attributeInfo.getCmpCpuCount(); ++i){
            String attr_key = "cmp_cpu";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpCpu(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpCpu(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_structure
        for(int i = 0; i < attributeInfo.getQcStructureCount(); ++i){
            String attr_key = "qc_structure";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcStructure(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcStructure(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of diseases
        for(int i = 0; i < attributeInfo.getDiseasesCount(); ++i){
            String attr_key = "diseases";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getDiseases(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getDiseases(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_area
        for(int i = 0; i < attributeInfo.getFinaAreaCount(); ++i){
            String attr_key = "fina_area";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaArea(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaArea(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of locations
        for(int i = 0; i < attributeInfo.getLocationsCount(); ++i){
            String attr_key = "locations";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getLocations(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getLocations(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of district
        for(int i = 0; i < attributeInfo.getDistrictCount(); ++i){
            String attr_key = "district";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getDistrict(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getDistrict(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of district
        for(int i = 0; i < attributeInfo.getQcManufacturersCount(); ++i){
            String attr_key = "qc_manufacturers";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcManufacturers(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcManufacturers(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_name
        for(int i = 0; i < attributeInfo.getFinaNameCount(); ++i){
            String attr_key = "fina_name";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaName(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaName(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_intake_form
        for(int i = 0; i < attributeInfo.getQcIntakeFormCount(); ++i){
            String attr_key = "qc_intake_form";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcIntakeForm(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcIntakeForm(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of material
        for(int i = 0; i < attributeInfo.getMaterialCount(); ++i){
            String attr_key = "material";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMaterial(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMaterial(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of cmp_model
        for(int i = 0; i < attributeInfo.getCmpModelCount(); ++i){
            String attr_key = "cmp_model";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpModel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpModel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of peoples
        if(attributeInfo.hasPeoples()){
            String attr_key = "peoples";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getPeoples()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getPeoples()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_level
        for(int i = 0; i < attributeInfo.getQcLevelCount(); ++i){
            String attr_key = "qc_level";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcLevel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcLevel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of ht_star_level
        for(int i = 0; i < attributeInfo.getHtStarLevelCount(); ++i){
            String attr_key = "ht_star_level";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getHtStarLevel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getHtStarLevel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of sizes
        for(int i = 0; i < attributeInfo.getSizesCount(); ++i){
            String attr_key = "sizes";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getSizes(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getSizes(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_type
        for(int i = 0; i < attributeInfo.getFinaTypeCount(); ++i){
            String attr_key = "fina_type";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaType(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaType(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of fina_region
        for(int i = 0; i < attributeInfo.getFinaRegionCount(); ++i){
            String attr_key = "fina_region";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaRegion(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaRegion(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cos_skin
        for(int i = 0; i < attributeInfo.getCosSkinCount(); ++i){
            String attr_key = "cos_skin";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCosSkin(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCosSkin(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_product
        for(int i = 0; i < attributeInfo.getFinaProductCount(); ++i){
            String attr_key = "fina_product";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaProduct(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaProduct(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_doors_num
        for(int i = 0; i < attributeInfo.getQcDoorsNumCount(); ++i){
            String attr_key = "qc_doors_num";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcDoorsNum(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcDoorsNum(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of qc_color
        for(int i = 0; i < attributeInfo.getQcColorCount(); ++i){
            String attr_key = "qc_color";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcColor(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcColor(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of fina_preEarnings
        for(int i = 0; i < attributeInfo.getFinaPreEarningsCount(); ++i){
            String attr_key = "fina_preEarnings";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaPreEarnings(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaPreEarnings(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_price
        for(int i = 0; i < attributeInfo.getQcPriceCount(); ++i){
            String attr_key = "qc_price";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcPrice(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcPrice(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of cmp_disk
        for(int i = 0; i < attributeInfo.getCmpDiskCount(); ++i){
            String attr_key = "cmp_disk";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpDisk(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpDisk(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of tk_class
        for(int i = 0; i < attributeInfo.getTkClassCount(); ++i){
            String attr_key = "tk_class";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getTkClass(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getTkClass(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of wares
        for(int i = 0; i < attributeInfo.getWaresCount(); ++i){
            String attr_key = "wares";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getWares(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getWares(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of shape
        for(int i = 0; i < attributeInfo.getShapeCount(); ++i){
            String attr_key = "shape";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getShape(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getShape(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of size
        for(int i = 0; i < attributeInfo.getSizeCount(); ++i){
            String attr_key = "size";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getSize(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getSize(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of colours
        for(int i = 0; i < attributeInfo.getColoursCount(); ++i){
            String attr_key = "colours";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getColours(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getColours(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of mdc_body
        for(int i = 0; i < attributeInfo.getMdcBodyCount(); ++i){
            String attr_key = "mdc_body";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMdcBody(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMdcBody(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_code
        for(int i = 0; i < attributeInfo.getFinaCodeCount(); ++i){
            String attr_key = "fina_code";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaCode(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaCode(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cmp_resolution
        for(int i = 0; i < attributeInfo.getCmpResolutionCount(); ++i){
            String attr_key = "cmp_resolution";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpResolution(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpResolution(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_used_car_price
        for(int i = 0; i < attributeInfo.getQcUsedCarPriceCount(); ++i){
            String attr_key = "qc_used_car_price";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcUsedCarPrice(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcUsedCarPrice(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of qc_cylinder
        for(int i = 0; i < attributeInfo.getQcCylinderCount(); ++i){
            String attr_key = "qc_cylinder";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcCylinder(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcCylinder(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of qc_model
        for(int i = 0; i < attributeInfo.getQcModelCount(); ++i){
            String attr_key = "qc_model";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcModel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcModel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_style
        for(int i = 0; i < attributeInfo.getFinaStyleCount(); ++i){
            String attr_key = "fina_style";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaStyle(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaStyle(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of price_level
        for(int i = 0; i < attributeInfo.getPriceLevelCount(); ++i){
            String attr_key = "price_level";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getPriceLevel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getPriceLevel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_deadline
        for(int i = 0; i < attributeInfo.getFinaDeadlineCount(); ++i){
            String attr_key = "fina_deadline";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaDeadline(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaDeadline(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of style
        for(int i = 0; i < attributeInfo.getStyleCount(); ++i){
            String attr_key = "style";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getStyle(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getStyle(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_gearbox
        for(int i = 0; i < attributeInfo.getQcGearboxCount(); ++i){
            String attr_key = "qc_gearbox";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcGearbox(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcGearbox(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of fina_market
        for(int i = 0; i < attributeInfo.getFinaMarketCount(); ++i){
            String attr_key = "fina_market";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaMarket(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaMarket(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_market
        for(int i = 0; i < attributeInfo.getCrowdCount(); ++i){
            String attr_key = "crowd";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCrowd(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCrowd(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_fuel
        for(int i = 0; i < attributeInfo.getQcFuelCount(); ++i){
            String attr_key = "qc_fuel";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcFuel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcFuel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_method
        for(int i = 0; i < attributeInfo.getFinaMethodCount(); ++i){
            String attr_key = "fina_method";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaMethod(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaMethod(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of price_num
        if(attributeInfo.hasPriceNum()){
            String attr_key = "price_num";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getPriceNum()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getPriceNum()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of cmp_screen
        for(int i = 0; i < attributeInfo.getCmpScreenCount(); ++i){
            String attr_key = "cmp_screen";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpScreen(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpScreen(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of clo_collar
        for(int i = 0; i < attributeInfo.getCloCollarCount(); ++i){
            String attr_key = "clo_collar";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCloCollar(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCloCollar(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of origin
        for(int i = 0; i < attributeInfo.getOriginCount(); ++i){
            String attr_key = "origin";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getOrigin(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getOrigin(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_newPrice
        for(int i = 0; i < attributeInfo.getFinaNewPriceCount(); ++i){
            String attr_key = "fina_newPrice";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaNewPrice(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaNewPrice(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mdc_country
        for(int i = 0; i < attributeInfo.getMdcCountryCount(); ++i){
            String attr_key = "mdc_country";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMdcCountry(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMdcCountry(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_company
        for(int i = 0; i < attributeInfo.getFinaCompanyCount(); ++i){
            String attr_key = "fina_company";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaCompany(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaCompany(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cmp_os
        for(int i = 0; i < attributeInfo.getCmpOsCount(); ++i){
            String attr_key = "cmp_os";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpOs(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpOs(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of food_wrap
        for(int i = 0; i < attributeInfo.getFoodWrapCount(); ++i){
            String attr_key = "food_wrap";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFoodWrap(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFoodWrap(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_seats_num
        for(int i = 0; i < attributeInfo.getQcSeatsNumCount(); ++i){
            String attr_key = "qc_seats_num";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcSeatsNum(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcSeatsNum(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cmp_color
        for(int i = 0; i < attributeInfo.getCmpColorCount(); ++i){
            String attr_key = "cmp_color";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpColor(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpColor(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of fina_range
        for(int i = 0; i < attributeInfo.getFinaRangeCount(); ++i){
            String attr_key = "fina_range";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFinaRange(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFinaRange(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of mdc_diseaseses
        for(int i = 0; i < attributeInfo.getMdcDiseaseCount(); ++i){
            String attr_key = "mdc_diseaseses";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMdcDisease(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMdcDisease(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of tour_type
        for(int i = 0; i < attributeInfo.getTourTypeCount(); ++i){
            String attr_key = "tour_type";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getTourType(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getTourType(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_drive
        for(int i = 0; i < attributeInfo.getQcDriveCount(); ++i){
            String attr_key = "qc_drive";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcDrive(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcDrive(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of sexes
        for(int i = 0; i < attributeInfo.getSexesCount(); ++i){
            String attr_key = "sexes";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getSexes(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getSexes(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of cmp_memory
        for(int i = 0; i < attributeInfo.getCmpMemoryCount(); ++i){
            String attr_key = "cmp_memory";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCmpMemory(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCmpMemory(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of qc_displacement
        for(int i = 0; i < attributeInfo.getQcDisplacementCount(); ++i){
            String attr_key = "qc_displacement";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getQcDisplacement(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getQcDisplacement(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of price_sum
        if(attributeInfo.hasPriceSum()){
            String attr_key = "price_sum";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getPriceSum()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getPriceSum()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of cos_smell
        for(int i = 0; i < attributeInfo.getCosSmellCount(); ++i){
            String attr_key = "cos_smell";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCosSmell(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCosSmell(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of min_price
        if(attributeInfo.hasMinPrice()){
            String attr_key = "min_price";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMinPrice()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMinPrice()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of max_price
        if(attributeInfo.hasMaxPrice()){
            String attr_key = "max_price";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMaxPrice()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMaxPrice()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of brands
        for(int i = 0; i < attributeInfo.getBrandsCount(); ++i){
            String attr_key = "brands";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBrands(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBrands(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of artist
        for(int i = 0; i < attributeInfo.getArtistCount(); ++i){
            String attr_key = "artist";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getArtist(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getArtist(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of actors
        for(int i = 0; i < attributeInfo.getActorsCount(); ++i){
            String attr_key = "actors";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getActors(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getActors(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mv_name
        for(int i = 0; i < attributeInfo.getMvNameCount(); ++i){
            String attr_key = "mv_name";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMvName(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMvName(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of directors
        for(int i = 0; i < attributeInfo.getDirectorsCount(); ++i){
            String attr_key = "directors";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getDirectors(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getDirectors(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mv_area
        for(int i = 0; i < attributeInfo.getMvAreaCount(); ++i){
            String attr_key = "mv_area";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMvArea(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMvArea(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mv_year
        for(int i = 0; i < attributeInfo.getMvYearCount(); ++i){
            String attr_key = "mv_year";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMvYear(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMvYear(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of authors
        for(int i = 0; i < attributeInfo.getAuthorsCount(); ++i){
            String attr_key = "authors";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getAuthors(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getAuthors(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }


        //get the distribution of bk_name
        for(int i = 0; i < attributeInfo.getBkNameCount(); ++i){
            String attr_key = "bk_name";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBkName(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBkName(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of bk_translate
        for(int i = 0; i < attributeInfo.getBkTranslateCount(); ++i){
            String attr_key = "bk_translate";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBkTranslate(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBkTranslate(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of bk_translate
        for(int i = 0; i < attributeInfo.getBkTranslateCount(); ++i){
            String attr_key = "bk_translate";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBkTranslate(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBkTranslate(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of bk_process
        for(int i = 0; i < attributeInfo.getBkProcessCount(); ++i){
            String attr_key = "bk_process";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBkProcess(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBkProcess(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of bk_group
        for(int i = 0; i < attributeInfo.getBkGroupCount(); ++i){
            String attr_key = "bk_group";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBkGroup(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBkGroup(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of bk_length
        for(int i = 0; i < attributeInfo.getBkLengthCount(); ++i){
            String attr_key = "bk_length";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getBkLength(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getBkLength(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of tastes
        for(int i = 0; i < attributeInfo.getTastesCount(); ++i){
            String attr_key = "tastes";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getTastes(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getTastes(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of materials
        for(int i = 0; i < attributeInfo.getMaterialsCount(); ++i){
            String attr_key = "materials";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMaterials(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMaterials(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of crowds
        for(int i = 0; i < attributeInfo.getCrowdsCount(); ++i){
            String attr_key = "crowds";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCrowds(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCrowds(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of crafts
        for(int i = 0; i < attributeInfo.getCraftsCount(); ++i){
            String attr_key = "crafts";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getCrafts(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getCrafts(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of visceras
        for(int i = 0; i < attributeInfo.getViscerasCount(); ++i){
            String attr_key = "visceras";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getVisceras(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getVisceras(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of functions
        for(int i = 0; i < attributeInfo.getFunctionsCount(); ++i){
            String attr_key = "functions";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFunctions(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFunctions(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of wares
        for(int i = 0; i < attributeInfo.getWaresCount(); ++i){
            String attr_key = "wares";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getWares(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getWares(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of styles
        for(int i = 0; i < attributeInfo.getStylesCount(); ++i){
            String attr_key = "styles";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getStyles(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getStyles(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of difficultys
        for(int i = 0; i < attributeInfo.getDifficultysCount(); ++i){
            String attr_key = "difficultys";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getDifficultys(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getDifficultys(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of scenes
        for(int i = 0; i < attributeInfo.getScenesCount(); ++i){
            String attr_key = "scenes";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getScenes(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getScenes(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of food_wrap
        for(int i = 0; i < attributeInfo.getFoodWrapCount(); ++i){
            String attr_key = "food_wrap";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFoodWrap(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFoodWrap(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of food_has_sugar
        for(int i = 0; i < attributeInfo.getFoodHasSugarCount(); ++i){
            String attr_key = "food_has_sugar";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getFoodHasSugar(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getFoodHasSugar(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_model
        for(int i = 0; i < attributeInfo.getMbModelCount(); ++i){
            String attr_key = "mb_model";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbModel(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbModel(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_date
        for(int i = 0; i < attributeInfo.getMbDateCount(); ++i){
            String attr_key = "mb_date";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbDate(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbDate(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_support
        for(int i = 0; i < attributeInfo.getMbSupportCount(); ++i){
            String attr_key = "mb_support";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){

                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbSupport(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbSupport(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_os
        for(int i = 0; i < attributeInfo.getMbOsCount(); ++i){
            String attr_key = "mb_os";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbOs(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbOs(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_resolution
        for(int i = 0; i < attributeInfo.getMbResolutionCount(); ++i){
            String attr_key = "mb_resolution";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbResolution(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbResolution(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_ram
        for(int i = 0; i < attributeInfo.getMbRamCount(); ++i){
            String attr_key = "mb_ram";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbRam(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbRam(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_rom
        for(int i = 0; i < attributeInfo.getMbRomCount(); ++i){
            String attr_key = "mb_rom";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbRom(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbRom(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_camera
        for(int i = 0; i < attributeInfo.getMbCameraCount(); ++i){
            String attr_key = "mb_camera";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbCamera(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbCamera(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_screen
        for(int i = 0; i < attributeInfo.getMbScreenCount(); ++i){
            String attr_key = "mb_screen";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbScreen(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbScreen(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }

        //get the distribution of mb_pattern
        for(int i = 0; i < attributeInfo.getMbPatternCount(); ++i){
            String attr_key = "mb_pattern";
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.add(attr_key);
            if(keyMap.containsKey(attr_key)){
                key_list.add(keyMap.get(attr_key));
                if(key_list.size() < 3){
                    key_list.add(String.valueOf(attributeInfo.getMbPattern(i).getValue()));
                }
                else{
                    key_list.set(2, String.valueOf(attributeInfo.getMbPattern(i).getValue()));
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);
            }
        }
    }



    /*
     *  get the Distribution of the first Category for long preferences
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, IndustryInfo,the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, map,save the results of the distribution of the first category
     *  @param: prefix, string, the prefix string of the first category（长期购物偏好以及长期兴趣偏好)
     *  @return: void
     */
    public static void getFirstCateDistribution(UserProfile2.IndustryInfo indus,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the first category for median
            List<String> key_list = new ArrayList<String>();
            //set the prefix string to the first position of the key_list
            key_list.add(prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);

                //get the all of attributes of the First Category of media
                if(first_cate.hasAttrs()){
                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);

                //get the all of attributes of the First Category of business
                if(first_cate.hasAttrs()){
                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FirstCategory的关系
            Map<String, UserProfile2.FirstCategory> str2firstCateMap = new HashMap<String, UserProfile2.FirstCategory>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                String key = StringUtils.join(key_list, "/");
                str2firstCateMap.put(key, first_cate);
                long timeStamp = first_cate.getTimestamp()*1000L;
                str2TimestampMap.put(key, timeStamp);
            }
            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2firstCateMap.containsKey(entry.getKey())){
                    UserProfile2.FirstCategory firstCategory = str2firstCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
                    if(firstCategory.hasAttrs()){
                        getAllOfAttributionDistribution(firstCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("potential_business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FirstCategory的关系
            Map<String, UserProfile2.FirstCategory> str2firstCateMap = new HashMap<String, UserProfile2.FirstCategory>();
            //按照前缀与权重进行存储
            Map<String, Integer> str2WeightMap = new TreeMap<String, Integer>();

            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                String key = StringUtils.join(key_list, "/");
                str2firstCateMap.put(key, first_cate);
                int weight = first_cate.getWeight();
                str2WeightMap.put(key, weight);
            }
            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重选取top5作为当下需求
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Integer> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2firstCateMap.containsKey(entry.getKey())){
                    UserProfile2.FirstCategory firstCategory = str2firstCateMap.get(entry.getKey());

                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
                    if(firstCategory.hasAttrs()){
                        getAllOfAttributionDistribution(firstCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("short_business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,first_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                long timeStamp = first_cate.getTimestamp()*1000L;

                if(timeStamp < threshold_time || threshold_time == 0){
                    continue;
                }
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                String key = StringUtils.join(key_list, "/");
                if(!result.containsKey("/短期购物偏好")){
                    result.put("/短期购物偏好",1);
                }
                result.put(key, 1);

                //get the all of attributes of the First Category of business
                if(first_cate.hasAttrs()){
                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
                }
            }
        }
        else if(type.equals("short_media")) {
            //get the distribution of the first category for median
            List<String> key_list = new ArrayList<String>();
            //set the prefix string to the first position of the key_list
            key_list.add(prefix);
            long threshold_time = System.currentTimeMillis() - 86400000L*90;
            //long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,first_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                long timeStamp = first_cate.getTimestamp()*1000L;
                if(timeStamp < threshold_time || threshold_time == 0){
                    continue;
                }
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                String key = StringUtils.join(key_list, "/");
                if(!result.containsKey("/短期兴趣偏好")){
                    result.put("/短期兴趣偏好",1);
                }
                result.put(key, 1);

                //get the all of attributes of the First Category of media
                if(first_cate.hasAttrs()){
                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
                }
            }
        }
    }

    public static void getFirstCateDistribution(PortraitClass.IndustryInfo indus,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        //get the distribution of the first category for median
        List<String> key_list = new ArrayList<String>();
        //set the prefix string to the first position of the key_list
        key_list.add(prefix);
        if(indus == null){
            return;
        }
        for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
            PortraitClass.Category first_cate = indus.getFirstCate(first_index);
            //judge whether the key_list contains the second element
            if(key_list.size() < 2){
                key_list.add(first_cate.getName());
            }
            else{
                key_list.set(1, first_cate.getName());
            }
            String key = StringUtils.join(key_list, "/");
            result.put(key, 1);

            //get the all of attributes of the First Category of media
//                if(first_cate.hasAttrs()){
//                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
//                }
        }
    }


    public static void getFirstCateDistribution(PortraitClass.CidInfo cidInfo,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the first category for median
            List<String> key_list = new ArrayList<String>();
            //set the prefix string to the first position of the key_list
            key_list.add(prefix);
            if((cidInfo == null) || (!cidInfo.hasMediaIndus())){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);

                //get the all of attributes of the First Category of media
//                if(first_cate.hasAttrs()){
//                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
//                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            if((cidInfo == null) || (!cidInfo.hasEcIndus())){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);

                //get the all of attributes of the First Category of business
//                if(first_cate.hasAttrs()){
//                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
//                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();
            if((cidInfo == null) || (!cidInfo.hasEcIndus())){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            //用来存储前缀与FirstCategory的关系
            Map<String, PortraitClass.Category> str2firstCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                String key = StringUtils.join(key_list, "/");
                str2firstCateMap.put(key, first_cate);
                long timeStamp = first_cate.getUpdateTime()*1000L;
                str2TimestampMap.put(key, timeStamp);
            }
            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2firstCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category firstCategory = str2firstCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
//                    if(firstCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(firstCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("potential_business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();

            if((cidInfo == null) || (!cidInfo.hasEcIndus())){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            //用来存储前缀与FirstCategory的关系
            Map<String, PortraitClass.Category> str2firstCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与权重进行存储
            Map<String, Double> str2WeightMap = new TreeMap<String, Double>();

            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                String key = StringUtils.join(key_list, "/");
                str2firstCateMap.put(key, first_cate);
                double weight = first_cate.getWeight();
                str2WeightMap.put(key, weight);
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重选取top5作为当下需求
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Double> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2firstCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category firstCategory = str2firstCateMap.get(entry.getKey());

                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
//                    if(firstCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(firstCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("short_business")){
            //get the distribution of the first category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,first_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null) || (!cidInfo.hasEcIndus())){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                long timeStamp = first_cate.getUpdateTime()*1000L;

                if(timeStamp < threshold_time || threshold_time == 0){
                    continue;
                }
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                String key = StringUtils.join(key_list, "/");
                if(!result.containsKey("/短期购物偏好")){
                    result.put("/短期购物偏好",1);
                }
                result.put(key, 1);

                //get the all of attributes of the First Category of business
//                if(first_cate.hasAttrs()){
//                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
//                }
            }
        }
        else if(type.equals("short_media")) {
            //get the distribution of the first category for median
            List<String> key_list = new ArrayList<String>();
            //set the prefix string to the first position of the key_list
            key_list.add(prefix);
            long threshold_time = System.currentTimeMillis() - 86400000L*90;
            //long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,first_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null) || (!cidInfo.hasMediaIndus())){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                long timeStamp = first_cate.getUpdateTime()*1000L;
                if(timeStamp < threshold_time || threshold_time == 0){
                    continue;
                }
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                String key = StringUtils.join(key_list, "/");
                if(!result.containsKey("/短期兴趣偏好")){
                    result.put("/短期兴趣偏好",1);
                }
                result.put(key, 1);

                //get the all of attributes of the First Category of media
//                if(first_cate.hasAttrs()){
//                    getAllOfAttributionDistribution(first_cate.getAttrs(), key, keyMap, result);
//                }
            }
        }
    }


    /*
     *  get the Distribution of the Second Category for long preferences
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, save the results of the distribution of the all categories
     *  @param: prefix, the prefix string of the second category(长期购物偏好和长期兴趣偏好)
     *  @return: void
     */
    public static void getSecondCateDistribution(UserProfile2.IndustryInfo indus,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the second category for median
            List<String> key_list = new ArrayList<String>();
            //key_list.set(0, prefix);
            key_list.add(prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    //judge whether the key_list contains the third element
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    result.put(key, 1);


                    //get the all of attributes of the Second Category of media
                    if(second_cate.hasAttrs()){
                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    result.put(key, 1);

                    //get the all of attributes of the Second Category of business
                    if(second_cate.hasAttrs()){
                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与SecondCategory的关系
            Map<String, UserProfile2.SecondCategory> str2secondCateMap = new HashMap<String, UserProfile2.SecondCategory>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();


            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }


                    String key = StringUtils.join(key_list, "/");
                    str2secondCateMap.put(key, second_cate);
                    long timeStamp = second_cate.getTimestamp()*1000L;
                    str2TimestampMap.put(key, timeStamp);

                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2secondCateMap.containsKey(entry.getKey())){
                    UserProfile2.SecondCategory secondCategory = str2secondCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
                    if(secondCategory.hasAttrs()){
                        getAllOfAttributionDistribution(secondCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }

        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与SecondCategory的关系
            Map<String, UserProfile2.SecondCategory> str2secondCateMap = new HashMap<String, UserProfile2.SecondCategory>();
            //按照前缀与权重进行存储
            Map<String, Integer> str2WeightMap = new TreeMap<String, Integer>();


            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }


                    String key = StringUtils.join(key_list, "/");
                    str2secondCateMap.put(key, second_cate);
                    int weight = second_cate.getWeight();
                    str2WeightMap.put(key, weight);

                }
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重大小选取top5作为当下需求
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Integer> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2secondCateMap.containsKey(entry.getKey())){
                    UserProfile2.SecondCategory secondCategory = str2secondCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
                    if(secondCategory.hasAttrs()){
                        getAllOfAttributionDistribution(secondCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }

        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);

            //key_list.set(0, prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,second_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);

                    long timeStamp = second_cate.getTimestamp()*1000L;
                    if(timeStamp < threshold_time || threshold_time == 0){
                        continue;
                    }
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    if(!result.containsKey("/短期购物偏好")){
                        result.put("/短期购物偏好",1);
                    }
                    result.put(key, 1);

                    //get the all of attributes of the Second Category of business
                    if(second_cate.hasAttrs()){
                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the second category for median
            List<String> key_list = new ArrayList<String>();
            //key_list.set(0, prefix);
            key_list.add(prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,second_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    //judge whether the key_list contains the third element

                    long timeStamp = second_cate.getTimestamp()*1000L;

                    if(timeStamp < threshold_time || threshold_time == 0){
                        continue;
                    }
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    if(!result.containsKey("/短期兴趣偏好")){
                        result.put("/短期兴趣偏好",1);
                    }
                    result.put(key, 1);


                    //get the all of attributes of the Second Category of media
                    if(second_cate.hasAttrs()){
                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
                    }
                }
            }
        }
    }

    public static void getSecondCateDistribution(PortraitClass.CidInfo cidInfo,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the second category for median
            List<String> key_list = new ArrayList<String>();
            //key_list.set(0, prefix);
            key_list.add(prefix);
            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    //judge whether the key_list contains the third element
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    result.put(key, 1);


                    //get the all of attributes of the Second Category of media
//                    if(second_cate.hasAttrs()){
//                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    result.put(key, 1);

                    //get the all of attributes of the Second Category of business
//                    if(second_cate.hasAttrs()){
//                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与SecondCategory的关系
            Map<String, PortraitClass.Category> str2secondCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();


            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }


                    String key = StringUtils.join(key_list, "/");
                    str2secondCateMap.put(key, second_cate);
                    long timeStamp = second_cate.getUpdateTime()*1000L;
                    str2TimestampMap.put(key, timeStamp);

                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2secondCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category secondCategory = str2secondCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
//                    if(secondCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(secondCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }

        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与SecondCategory的关系
            Map<String, PortraitClass.Category> str2secondCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与权重进行存储
            Map<String, Double> str2WeightMap = new TreeMap<String, Double>();

            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }


                    String key = StringUtils.join(key_list, "/");
                    str2secondCateMap.put(key, second_cate);
                    double weight = second_cate.getWeight();
                    str2WeightMap.put(key, weight);

                }
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重大小选取top5作为当下需求
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Double> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2secondCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category secondCategory = str2secondCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
//                    if(secondCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(secondCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }

        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);

            //key_list.set(0, prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,second_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);

                    long timeStamp = second_cate.getUpdateTime()*1000L;
                    if(timeStamp < threshold_time || threshold_time == 0){
                        continue;
                    }
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    if(!result.containsKey("/短期购物偏好")){
                        result.put("/短期购物偏好",1);
                    }
                    result.put(key, 1);

                    //get the all of attributes of the Second Category of business
//                    if(second_cate.hasAttrs()){
//                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the second category for median
            List<String> key_list = new ArrayList<String>();
            //key_list.set(0, prefix);
            key_list.add(prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,second_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                //judge whether the key_list contains the second element
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    //judge whether the key_list contains the third element

                    long timeStamp = second_cate.getUpdateTime()*1000L;

                    if(timeStamp < threshold_time || threshold_time == 0){
                        continue;
                    }
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    if(!result.containsKey("/短期兴趣偏好")){
                        result.put("/短期兴趣偏好",1);
                    }
                    result.put(key, 1);


                    //get the all of attributes of the Second Category of media
//                    if(second_cate.hasAttrs()){
//                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
//                    }
                }
            }
        }
    }

    public static void getSecondCateDistribution(PortraitClass.IndustryInfo indus,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){

        //get the distribution of the second category for median
        List<String> key_list = new ArrayList<String>();
        //key_list.set(0, prefix);
        key_list.add(prefix);
        if(indus == null){
            return;
        }

        for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
            PortraitClass.Category first_cate = indus.getFirstCate(first_index);
            //judge whether the key_list contains the second element
            if(key_list.size() < 2){
                key_list.add(first_cate.getName());
            }
            else{
                key_list.set(1, first_cate.getName());
            }
            //key_list.set(1, first_cate.getName());
            for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                //judge whether the key_list contains the third element
                if(key_list.size() < 3){
                    key_list.add(second_cate.getName());
                }
                else{
                    key_list.set(2, second_cate.getName());
                }
                //key_list.set(2, second_cate.getName());
                String key = StringUtils.join(key_list, "/");
                result.put(key, 1);


                //get the all of attributes of the Second Category of media
//                    if(second_cate.hasAttrs()){
//                        getAllOfAttributionDistribution(second_cate.getAttrs(), key, keyMap, result);
//                    }
            }
        }
    }

    /*
     *  get the Distribution of the Third Category for long preferences
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, save the results of the distribution of the all categories
     *  @param: prefix, the prefix string of the third category(长期购物偏好和长期兴趣偏好)
     *  @return: void
     */
    public static void getThirdCateDistribution(UserProfile2.IndustryInfo indus,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        result.put(key, 1);
                        //get the all of attributes of the Third Category of media
                        if(third_cate.hasAttrs()){
                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
                        }
                    }
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        result.put(key, 1);

                        //get the all of attributes of the Third Category of business
                        if(third_cate.hasAttrs()){
                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
                        }
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与ThirdCategory的关系
            Map<String, UserProfile2.ThirdCategory> str2thirdCateMap = new HashMap<String, UserProfile2.ThirdCategory>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        str2thirdCateMap.put(key, third_cate);
                        long timeStamp = third_cate.getTimestamp()*1000L;
                        str2TimestampMap.put(key, timeStamp);

                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2thirdCateMap.containsKey(entry.getKey())){
                    UserProfile2.ThirdCategory thirdCategory = str2thirdCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
                    if(thirdCategory.hasAttrs()){
                        getAllOfAttributionDistribution(thirdCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }

        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FirstCategory的关系
            Map<String, UserProfile2.ThirdCategory> str2thirdCateMap = new HashMap<String, UserProfile2.ThirdCategory>();
            //按照前缀与权重进行存储
            Map<String, Integer> str2WeightMap = new TreeMap<String, Integer>();

            key_list.add(prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        str2thirdCateMap.put(key, third_cate);
                        int weight = third_cate.getWeight();
                        //long timeStamp = first_cate.getTimestamp()*1000L;
                        str2WeightMap.put(key, weight);

                    }
                }
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为潜在需求特征
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Integer> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2thirdCateMap.containsKey(entry.getKey())){
                    UserProfile2.ThirdCategory thirdCategory = str2thirdCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
                    if(thirdCategory.hasAttrs()){
                        getAllOfAttributionDistribution(thirdCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }

        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            long threshold_time = 0L;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,third_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);

                        long timeStamp = third_cate.getTimestamp()*1000L;
                        if(timeStamp < threshold_time || threshold_time == 0){
                            continue;
                        }
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        if(!result.containsKey("/短期购物偏好")){
                            result.put("/短期购物偏好",1);
                        }

                        result.put(key, 1);

                        //get the all of attributes of the Third Category of business
                        if(third_cate.hasAttrs()){
                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
                        }
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            long threshold_time = 0L;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,third_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);

                        long timeStamp = third_cate.getTimestamp()*1000L;
                        if(timeStamp < threshold_time || threshold_time == 0){
                            continue;
                        }
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");

                        if(!result.containsKey("/短期兴趣偏好")){
                            result.put("/短期兴趣偏好",1);
                        }
                        result.put(key, 1);
                        //get the all of attributes of the Third Category of media
                        if(third_cate.hasAttrs()){
                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
                        }
                    }
                }
            }
        }

    }

    public static void getThirdCateDistribution(PortraitClass.CidInfo cidInfo,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        result.put(key, 1);
                        //get the all of attributes of the Third Category of media
//                        if(third_cate.hasAttrs()){
//                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
//                        }
                    }
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        result.put(key, 1);

                        //get the all of attributes of the Third Category of business
//                        if(third_cate.hasAttrs()){
//                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
//                        }
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与ThirdCategory的关系
            Map<String, PortraitClass.Category> str2thirdCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        str2thirdCateMap.put(key, third_cate);
                        long timeStamp = third_cate.getUpdateTime()*1000L;
                        str2TimestampMap.put(key, timeStamp);

                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2thirdCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category thirdCategory = str2thirdCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
//                    if(thirdCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(thirdCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }

        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FirstCategory的关系
            Map<String, PortraitClass.Category> str2thirdCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与权重进行存储
            Map<String, Double> str2WeightMap = new TreeMap<String, Double>();

            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        str2thirdCateMap.put(key, third_cate);
                        double weight = third_cate.getWeight();
                        //long timeStamp = first_cate.getTimestamp()*1000L;
                        str2WeightMap.put(key, weight);

                    }
                }
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为潜在需求特征
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Double> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2thirdCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category thirdCategory = str2thirdCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
//                    if(thirdCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(thirdCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }

        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            long threshold_time = 0L;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,third_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }


            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);

                        long timeStamp = third_cate.getUpdateTime()*1000L;
                        if(timeStamp < threshold_time || threshold_time == 0){
                            continue;
                        }
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        if(!result.containsKey("/短期购物偏好")){
                            result.put("/短期购物偏好",1);
                        }

                        result.put(key, 1);

                        //get the all of attributes of the Third Category of business
//                        if(third_cate.hasAttrs()){
//                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
//                        }
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            long threshold_time = 0L;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,third_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);

                        long timeStamp = third_cate.getUpdateTime()*1000L;
                        if(timeStamp < threshold_time || threshold_time == 0){
                            continue;
                        }
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        String key = StringUtils.join(key_list, "/");

                        if(!result.containsKey("/短期兴趣偏好")){
                            result.put("/短期兴趣偏好",1);
                        }
                        result.put(key, 1);
                        //get the all of attributes of the Third Category of media
//                        if(third_cate.hasAttrs()){
//                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
//                        }
                    }
                }
            }
        }

    }

    public static void getThirdCateDistribution(PortraitClass.IndustryInfo indus,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        //get the distribution of the third category for median
        List<String> key_list = new ArrayList<String>();
        key_list.add(prefix);
        //key_list.set(0, prefix);

        if(indus == null){
            return;
        }

        for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index) {
            PortraitClass.Category first_cate = indus.getFirstCate(first_index);
            if (key_list.size() < 2) {
                key_list.add(first_cate.getName());
            } else {
                key_list.set(1, first_cate.getName());
            }
            //key_list.set(1, first_cate.getName());
            for (int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index) {
                PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                if (key_list.size() < 3) {
                    key_list.add(second_cate.getName());
                } else {
                    key_list.set(2, second_cate.getName());
                }
                //key_list.set(2, second_cate.getName());
                for (int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index) {
                    PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                    if (key_list.size() < 4) {
                        key_list.add(third_cate.getName());
                    } else {
                        key_list.set(3, third_cate.getName());
                    }
                    //key_list.set(3, third_cate.getName());
                    String key = StringUtils.join(key_list, "/");
                    result.put(key, 1);
                    //get the all of attributes of the Third Category of media
//                        if(third_cate.hasAttrs()){
//                            getAllOfAttributionDistribution(third_cate.getAttrs(), key, keyMap, result);
//                        }
                }
            }
        }

    }

    /*
    *  get the Distribution of the Fourth Category for long preferences
    *  @param: type, string, indicate the type of category(media, business)
    *  @param: indus, the object of IndustryInfo, cid = Cbaifendian
    *  @param: result, save the results of the distribution of the all categories
    *  @param: prefix, the prefix string of the Fifth category(长期购物偏好和长期兴趣偏好)
    *  @return: void
    */
    public static void getFifthCateDistribution(UserProfile2.IndustryInfo indus,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getFiveCateCount(); ++fifth_index){
                                UserProfile2.FiveCategory fifth_cate = fourth_cate.getFiveCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of media
                                if(fifth_cate.hasAttrs()){
                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
                                }
                            }

                        }
                    }
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getFiveCateCount(); ++fifth_index) {
                                UserProfile2.FiveCategory fifth_cate = fourth_cate.getFiveCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of business
                                if(first_cate.hasAttrs()){
                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
                                }
                            }

                        }
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FiveCategory的关系
            Map<String, UserProfile2.FiveCategory> str2fiveCateMap = new HashMap<String, UserProfile2.FiveCategory>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getFiveCateCount(); ++fifth_index) {
                                UserProfile2.FiveCategory fifth_cate = fourth_cate.getFiveCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                str2fiveCateMap.put(key, fifth_cate);
                                long timeStamp = fifth_cate.getTimestamp()*1000L;
                                str2TimestampMap.put(key, timeStamp);
                            }
                        }
                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fiveCateMap.containsKey(entry.getKey())){
                    UserProfile2.FiveCategory fiveCategory = str2fiveCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
                    if(fiveCategory.hasAttrs()){
                        getAllOfAttributionDistribution(fiveCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FiveCategory的关系
            Map<String, UserProfile2.FiveCategory> str2fiveCateMap = new HashMap<String, UserProfile2.FiveCategory>();
            //按照前缀与权重进行存储
            Map<String, Integer> str2WeightMap = new TreeMap<String, Integer>();

            key_list.add(prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getFiveCateCount(); ++fifth_index) {
                                UserProfile2.FiveCategory fifth_cate = fourth_cate.getFiveCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                str2fiveCateMap.put(key, fifth_cate);
                                int weight = fifth_cate.getWeight();
                                str2WeightMap.put(key, weight);
                            }
                        }
                    }
                }
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重选取top5作为潜在需求特征
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Integer> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fiveCateMap.containsKey(entry.getKey())){
                    UserProfile2.FiveCategory fiveCategory = str2fiveCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
                    if(fiveCategory.hasAttrs()){
                        getAllOfAttributionDistribution(fiveCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,fifth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getFiveCateCount(); ++fifth_index) {
                                UserProfile2.FiveCategory fifth_cate = fourth_cate.getFiveCate(fifth_index);

                                long timeStamp = fifth_cate.getTimestamp()*1000L;
                                if(timeStamp < threshold_time || threshold_time == 0){
                                    continue;
                                }
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                if(!result.containsKey("/短期购物偏好")){
                                    result.put("/短期购物偏好",1);
                                }
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of business
                                if(first_cate.hasAttrs()){
                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
                                }
                            }

                        }
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the third category for median
            long threshold_time = 0L;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,fifth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getFiveCateCount(); ++fifth_index){
                                UserProfile2.FiveCategory fifth_cate = fourth_cate.getFiveCate(fifth_index);

                                long timeStamp = fifth_cate.getTimestamp()*1000L;
                                if(timeStamp < threshold_time || threshold_time == 0){
                                    continue;
                                }
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                if(!result.containsKey("/短期兴趣偏好")){
                                    result.put("/短期兴趣偏好",1);
                                }
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of media
                                if(fifth_cate.hasAttrs()){
                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
                                }
                            }

                        }
                    }
                }
            }
        }
    }


    public static void getFifthCateDistribution(PortraitClass.CidInfo cidInfo,
                                                     String type,
                                                     String prefix,
                                                     Map<String, String> keyMap,
                                                     Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index){
                                PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of media
//                                if(fifth_cate.hasAttrs()){
//                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
//                                }
                            }

                        }
                    }
                }
            }
        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index) {
                                PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of business
//                                if(first_cate.hasAttrs()){
//                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
//                                }
                            }

                        }
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FiveCategory的关系
            Map<String, PortraitClass.Category> str2fiveCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index) {
                                PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                str2fiveCateMap.put(key, fifth_cate);
                                long timeStamp = fifth_cate.getUpdateTime() * 1000L;
                                str2TimestampMap.put(key, timeStamp);
                            }
                        }
                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fiveCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category fiveCategory = str2fiveCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
//                    if(fiveCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(fiveCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FiveCategory的关系
            Map<String, PortraitClass.Category> str2fiveCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与权重进行存储
            Map<String, Double> str2WeightMap = new TreeMap<String, Double>();

            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }

                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index) {
                                PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                str2fiveCateMap.put(key, fifth_cate);
                                double weight = fifth_cate.getWeight();
                                str2WeightMap.put(key, weight);
                            }
                        }
                    }
                }
            }

            //按照权重（value)进行逆序排序
            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重选取top5作为潜在需求特征
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Double> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fiveCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category fiveCategory = str2fiveCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
//                    if(fiveCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(fiveCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,fifth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index) {
                                PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);

                                long timeStamp = fifth_cate.getUpdateTime() * 1000L;
                                if(timeStamp < threshold_time || threshold_time == 0){
                                    continue;
                                }
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                if(!result.containsKey("/短期购物偏好")){
                                    result.put("/短期购物偏好",1);
                                }
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of business
//                                if(first_cate.hasAttrs()){
//                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
//                                }
                            }

                        }
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the third category for median
            long threshold_time = 0L;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,fifth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index){
                                PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);

                                long timeStamp = fifth_cate.getUpdateTime() * 1000L;
                                if(timeStamp < threshold_time || threshold_time == 0){
                                    continue;
                                }
                                if(key_list.size() < 6){
                                    key_list.add(fifth_cate.getName());
                                }
                                else{
                                    key_list.set(5, fifth_cate.getName());
                                }
                                String key = StringUtils.join(key_list, "/");
                                if(!result.containsKey("/短期兴趣偏好")){
                                    result.put("/短期兴趣偏好",1);
                                }
                                result.put(key, 1);

                                //get the all of attributes of the Fifth Category of media
//                                if(fifth_cate.hasAttrs()){
//                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
//                                }
                            }

                        }
                    }
                }
            }
        }
    }

    public static void getFifthCateDistribution(PortraitClass.IndustryInfo indus,
                                                String type,
                                                String prefix,
                                                Map<String, String> keyMap,
                                                Map<String, Integer> result){

        //get the distribution of the third category for median
        List<String> key_list = new ArrayList<String>();
        key_list.add(prefix);
        //key_list.set(0, prefix);

        if(indus == null){
            return;
        }

        for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
            PortraitClass.Category first_cate = indus.getFirstCate(first_index);
            if(key_list.size() < 2){
                key_list.add(first_cate.getName());
            }
            else{
                key_list.set(1, first_cate.getName());
            }
            //key_list.set(1, first_cate.getName());
            for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                if(key_list.size() < 3){
                    key_list.add(second_cate.getName());
                }
                else{
                    key_list.set(2, second_cate.getName());
                }
                //key_list.set(2, second_cate.getName());
                for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                    PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                    if(key_list.size() < 4){
                        key_list.add(third_cate.getName());
                    }
                    else{
                        key_list.set(3, third_cate.getName());
                    }
                    //key_list.set(3, third_cate.getName());
                    for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                        PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                        if(key_list.size() < 5){
                            key_list.add(fourth_cate.getName());
                        }
                        else{
                            key_list.set(4, fourth_cate.getName());
                        }
                        //key_list.set(4, fourth_cate.getName());
                        for(int fifth_index = 0; fifth_index < fourth_cate.getNextCateCount(); ++fifth_index){
                            PortraitClass.Category fifth_cate = fourth_cate.getNextCate(fifth_index);
                            if(key_list.size() < 6){
                                key_list.add(fifth_cate.getName());
                            }
                            else{
                                key_list.set(5, fifth_cate.getName());
                            }
                            String key = StringUtils.join(key_list, "/");
                            result.put(key, 1);

                            //get the all of attributes of the Fifth Category of media
//                                if(fifth_cate.hasAttrs()){
//                                    getAllOfAttributionDistribution(fifth_cate.getAttrs(), key, keyMap, result);
//                                }
                        }

                    }
                }
            }
        }

    }

    /*
     *  get the Distribution of the Fourth Category for long preferences
     *  @param: type, string, indicate the type of category(media, business)
     *  @param: indus, the object of IndustryInfo, cid = Cbaifendian
     *  @param: result, save the results of the distribution of the all categories
     *  @param: prefix, the prefix string of the fourth category(长期购物偏好和长期兴趣偏好)
     *  @return: void
     */
    public static void getFourthCateDistribution(UserProfile2.IndustryInfo indus,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of media
                            if(fourth_cate.hasAttrs()){
                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
                            }

                        }
                    }
                }
            }

        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of business
                            if(fourth_cate.hasAttrs()){
                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
                            }
                        }
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FirstCategory的关系
            Map<String, UserProfile2.FourthCategory> str2fourthCateMap = new HashMap<String, UserProfile2.FourthCategory>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            str2fourthCateMap.put(key, fourth_cate);
                            long timeStamp = fourth_cate.getTimestamp()*1000L;
                            str2TimestampMap.put(key, timeStamp);
                        }
                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fourthCateMap.containsKey(entry.getKey())){
                    UserProfile2.FourthCategory fourthCategory = str2fourthCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
                    if(fourthCategory.hasAttrs()){
                        getAllOfAttributionDistribution(fourthCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FourthCategory的关系
            Map<String, UserProfile2.FourthCategory> str2fourthCateMap = new HashMap<String, UserProfile2.FourthCategory>();
            //按照前缀与权重进行存储
            Map<String, Integer> str2WeightMap = new TreeMap<String, Integer>();

            key_list.add(prefix);
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            str2fourthCateMap.put(key, fourth_cate);
                            int weight = fourth_cate.getWeight();
                            str2WeightMap.put(key, weight);
                        }
                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重选取top5作为潜在需求特征
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Integer> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fourthCateMap.containsKey(entry.getKey())){
                    UserProfile2.FourthCategory fourthCategory = str2fourthCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
                    if(fourthCategory.hasAttrs()){
                        getAllOfAttributionDistribution(fourthCategory.getAttrs(), entry.getKey(), keyMap, result);
                    }
                }
            }
        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,fourth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);

                            long timeStamp = fourth_cate.getTimestamp()*1000L;
                            if(timeStamp < threshold_time || threshold_time == 0){
                                continue;
                            }
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            if(!result.containsKey("/短期购物偏好")){
                                result.put("/短期购物偏好", 1);
                            }
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of business
                            if(fourth_cate.hasAttrs()){
                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
                            }
                        }
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,fourth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }
            for(int first_index = 0; first_index < indus.getMediaCateCount(); ++first_index){
                UserProfile2.FirstCategory first_cate = indus.getMediaCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getSecondCateCount(); ++second_index){
                    UserProfile2.SecondCategory second_cate = first_cate.getSecondCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getThirdCateCount(); ++third_index){
                        UserProfile2.ThirdCategory third_cate = second_cate.getThirdCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getFourthCateCount(); ++fourth_index){
                            UserProfile2.FourthCategory fourth_cate = third_cate.getFourthCate(fourth_index);

                            long timeStamp = fourth_cate.getTimestamp()*1000L;
                            if(timeStamp < threshold_time || threshold_time == 0){
                                continue;
                            }
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            if(!result.containsKey("/短期兴趣偏好")){
                                result.put("/短期兴趣偏好", 1);
                            }
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of media
                            if(fourth_cate.hasAttrs()){
                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
                            }

                        }
                    }
                }
            }

        }
    }

    public static void getFourthCateDistribution(PortraitClass.CidInfo cidInfo,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){

        if(type.equals("media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of media
//                            if(fourth_cate.hasAttrs()){
//                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
//                            }

                        }
                    }
                }
            }

        }
        else if(type.equals("business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){

                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of business
//                            if(fourth_cate.hasAttrs()){
//                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
//                            }
                        }
                    }
                }
            }
        }
        else if(type.equals("current_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FirstCategory的关系
            Map<String, PortraitClass.Category> str2fourthCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与时间戳进行存储
            Map<String, Long> str2TimestampMap = new TreeMap<String, Long>();

            key_list.add(prefix);
            //key_list.set(0, prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            str2fourthCateMap.put(key, fourth_cate);
                            long timeStamp = fourth_cate.getUpdateTime()*1000L;
                            str2TimestampMap.put(key, timeStamp);
                        }
                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(str2TimestampMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照时间戳选取top2作为当下需求
            int currentNum = 0, selectNum = 2;
            for(Map.Entry<String, Long> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fourthCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category fourthCategory = str2fourthCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/当下需求特征")){
                        result.put("/当下需求特征",1);
                    }
//                    if(fourthCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(fourthCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("potential_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();

            //用来存储前缀与FourthCategory的关系
            Map<String, PortraitClass.Category> str2fourthCateMap = new HashMap<String, PortraitClass.Category>();
            //按照前缀与权重进行存储
            Map<String, Double> str2WeightMap = new TreeMap<String, Double>();

            key_list.add(prefix);

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();

            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            str2fourthCateMap.put(key, fourth_cate);
                            double weight = fourth_cate.getWeight();
                            str2WeightMap.put(key, weight);
                        }
                    }
                }
            }

            //按照时间戳（value)进行逆序排序
            List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(str2WeightMap.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Double>>(){
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2){
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //按照权重选取top5作为潜在需求特征
            int currentNum = 0, selectNum = 5;
            for(Map.Entry<String, Double> entry : list){
                if(currentNum == selectNum){
                    break;
                }
                currentNum += 1;
                if(str2fourthCateMap.containsKey(entry.getKey())){
                    PortraitClass.Category fourthCategory = str2fourthCateMap.get(entry.getKey());
                    result.put(entry.getKey(), 1);
                    if(!result.containsKey("/潜在需求特征")){
                        result.put("/潜在需求特征",1);
                    }
//                    if(fourthCategory.hasAttrs()){
//                        getAllOfAttributionDistribution(fourthCategory.getAttrs(), entry.getKey(), keyMap, result);
//                    }
                }
            }
        }
        else if(type.equals("short_business")){
            //get the distribution of the second category for business
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_business,fourth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null || (!cidInfo.hasEcIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getEcIndus();
            //key_list.set(0, prefix);
            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);

                            long timeStamp = fourth_cate.getUpdateTime() * 1000L;
                            if(timeStamp < threshold_time || threshold_time == 0){
                                continue;
                            }
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            if(!result.containsKey("/短期购物偏好")){
                                result.put("/短期购物偏好",1);
                            }
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of business
//                            if(fourth_cate.hasAttrs()){
//                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
//                            }
                        }
                    }
                }
            }
        }
        else if(type.equals("short_media")){
            //get the distribution of the third category for median
            List<String> key_list = new ArrayList<String>();
            key_list.add(prefix);
            //key_list.set(0, prefix);
            long threshold_time = 0;
            try{
                //The current timestamp
                long currentTime = System.currentTimeMillis();
                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String d = df.format(currentTime);
                Date date = df.parse(d);
                threshold_time = date.getTime() - 86400000L*90;
                //threshold_time = 1;
            }catch(ParseException e){
                //e.printStackTrace();
                LOG.warn("short_meida,fourth_category threshold_time failed.");
                LOG.warn(e.getMessage());
            }

            if((cidInfo == null || (!cidInfo.hasMediaIndus()))){
                return;
            }
            PortraitClass.IndustryInfo indus = cidInfo.getMediaIndus();

            for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
                PortraitClass.Category first_cate = indus.getFirstCate(first_index);
                if(key_list.size() < 2){
                    key_list.add(first_cate.getName());
                }
                else{
                    key_list.set(1, first_cate.getName());
                }
                //key_list.set(1, first_cate.getName());
                for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                    PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                    if(key_list.size() < 3){
                        key_list.add(second_cate.getName());
                    }
                    else{
                        key_list.set(2, second_cate.getName());
                    }
                    //key_list.set(2, second_cate.getName());
                    for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                        PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                        if(key_list.size() < 4){
                            key_list.add(third_cate.getName());
                        }
                        else{
                            key_list.set(3, third_cate.getName());
                        }
                        //key_list.set(3, third_cate.getName());
                        for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                            PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);

                            long timeStamp = fourth_cate.getUpdateTime() * 1000L;
                            if(timeStamp < threshold_time || threshold_time == 0){
                                continue;
                            }
                            if(key_list.size() < 5){
                                key_list.add(fourth_cate.getName());
                            }
                            else{
                                key_list.set(4, fourth_cate.getName());
                            }
                            //key_list.set(4, fourth_cate.getName());
                            String key = StringUtils.join(key_list, "/");
                            if(!result.containsKey("/短期兴趣偏好")){
                                result.put("/短期兴趣偏好",1);
                            }
                            result.put(key, 1);

                            //get the all of attributes of the Fourth Category of media
//                            if(fourth_cate.hasAttrs()){
//                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
//                            }

                        }
                    }
                }
            }

        }
    }

    public static void getFourthCateDistribution(PortraitClass.IndustryInfo indus,
                                                 String type,
                                                 String prefix,
                                                 Map<String, String> keyMap,
                                                 Map<String, Integer> result){

        //get the distribution of the third category for median
        List<String> key_list = new ArrayList<String>();
        key_list.add(prefix);
        //key_list.set(0, prefix);

        if(indus == null){
            return;
        }

        for(int first_index = 0; first_index < indus.getFirstCateCount(); ++first_index){
            PortraitClass.Category first_cate = indus.getFirstCate(first_index);
            if(key_list.size() < 2){
                key_list.add(first_cate.getName());
            }
            else{
                key_list.set(1, first_cate.getName());
            }
            //key_list.set(1, first_cate.getName());
            for(int second_index = 0; second_index < first_cate.getNextCateCount(); ++second_index){
                PortraitClass.Category second_cate = first_cate.getNextCate(second_index);
                if(key_list.size() < 3){
                    key_list.add(second_cate.getName());
                }
                else{
                    key_list.set(2, second_cate.getName());
                }
                //key_list.set(2, second_cate.getName());
                for(int third_index = 0; third_index < second_cate.getNextCateCount(); ++third_index){
                    PortraitClass.Category third_cate = second_cate.getNextCate(third_index);
                    if(key_list.size() < 4){
                        key_list.add(third_cate.getName());
                    }
                    else{
                        key_list.set(3, third_cate.getName());
                    }
                    //key_list.set(3, third_cate.getName());
                    for(int fourth_index = 0; fourth_index < third_cate.getNextCateCount(); ++fourth_index){
                        PortraitClass.Category fourth_cate = third_cate.getNextCate(fourth_index);
                        if(key_list.size() < 5){
                            key_list.add(fourth_cate.getName());
                        }
                        else{
                            key_list.set(4, fourth_cate.getName());
                        }
                        //key_list.set(4, fourth_cate.getName());
                        String key = StringUtils.join(key_list, "/");
                        result.put(key, 1);

                        //get the all of attributes of the Fourth Category of media
//                            if(fourth_cate.hasAttrs()){
//                                getAllOfAttributionDistribution(fourth_cate.getAttrs(), key, keyMap, result);
//                            }

                    }
                }
            }
        }

    }


}
