package cn.bfd.tools;

import cn.bfd.protobuf.PortraitClass;
import cn.bfd.protobuf.UserProfile2;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetMarketingFeaturesAttribute {
	
	private static Logger LOG = Logger.getLogger(GetMarketingFeaturesAttribute.class);
	

	public static void getFirstClass(UserProfile2.UserProfile up,
									 String prefix,
									 Map<String, Integer> result){
		if(up.hasMarketFt()){
			result.put(prefix, 1);
		}
	}

	public static void getFirstClass(PortraitClass.MarketingFeatures mf,
									 String prefix,
									 Map<String, Integer> result){
		if(mf != null){
			result.put(prefix, 1);
		}
	}

	public static void getSecondClass(UserProfile2.UserProfile up,
									 String prefix,
									 Map<String, Integer> result){
		if(up.hasMarketFt()){
			result.put(prefix + "/营销", 1);
		}
	}

	public static void getSecondClass(PortraitClass.MarketingFeatures mf,
									  String prefix,
									  Map<String, Integer> result){
		if(mf != null){
			result.put(prefix + "/营销", 1);
		}
	}
	
	
	
	/*
     *  get the Distribution of DemographicInfo
     *  @param: dg_info, the object of DemographicInfo
     *  @param: result, save the results of the distribution of DemographicInfo
     *  @param: prefix, the prefix string of the Fifth category(/消费层级)
     *  @return: void
     */
    public static void getPriceLevelMarketingFeaturesDistribution(UserProfile2.MarketingFeatures mf,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
  	    String attr_key = "消费层级";
  	  	for(int i=0;i<mf.getConLevelNewCount();i++){
  	  		List<String> key_list = new ArrayList<String>();
  	  		key_list.add(prefix);
  	  		//LOG.info(prefix);
  	  		key_list.add(attr_key);
  	  	    //LOG.info(mf.getPriceLevel(i).getValue());
	        key_list.add("等级" + String.valueOf(mf.getConLevelNew(i).getValue()));
	        String key = StringUtils.join(key_list, "/");
	        //LOG.info(key);
	        result.put(key, 1);
  	  	  }
  	  }

	public static void getPriceLevelMarketingFeaturesDistribution(PortraitClass.MarketingFeatures mf,
																  String prefix,
																  Map<String, Integer> result){
		String attr_key = "消费层级";
		if(mf.hasPriceLevel()){
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			//LOG.info(prefix);
			key_list.add(attr_key);
			//LOG.info(mf.getPriceLevel(i).getValue());
			key_list.add("等级" + String.valueOf(mf.getPriceLevel().getValue()));
			String key = StringUtils.join(key_list, "/");
			//LOG.info(key);
			result.put(key, 1);
		}
	}

	/*
     *  get the old price level of MarketingFeatures
     *  @param: mf, the object of MarketingFeatures
     *  @param: result, save the results of the distribution of MarketingFeatures
     *  @param: prefix, the prefix string of the Fifth category(/消费层级)
     *  @return: void
     */
	public static void getOldPriceLevelMarketingFeaturesDistribution(UserProfile2.MarketingFeatures mf,
																	 String prefix,
																	 Map<String, Integer> result){
		String attr_key = "消费等级";
		for(int i=0;i<mf.getPriceLevelCount();i++){
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			//LOG.info(prefix);
			key_list.add(attr_key);
			//LOG.info(mf.getPriceLevel(i).getValue());
			key_list.add("等级" + String.valueOf(mf.getPriceLevel(i).getValue()));
			String key = StringUtils.join(key_list, "/");
			//LOG.info(key);
			result.put(key, 1);
		}
	}

    
    
    /*
     *  get the Distribution of DemographicInfo
     *  @param: dg_info, the object of DemographicInfo
     *  @param: result, save the results of the distribution of DemographicInfo
     *  @param: prefix, the prefix string of the Fifth category(/消费周期)
     *  @return: void
     */
    public static void getConPeriodMarketingFeaturesDistribution(UserProfile2.MarketingFeatures mf,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
  	      //String attr_key = "mf_cpn";
  	      String attr_key ="消费周期";
		  //key map
//		  Map<String, String> valMap = new HashMap<String, String>();
//		  valMap.put("1", "1周以内");
//		  valMap.put("2", "2周以内");
//		  valMap.put("3","1");
//		  valMap.put("4","4周以内");
//		  valMap.put("5", "一周以内");
  	      //LOG.info(attr_key);
  	  	  for(int i=0;i<mf.getConPeriodNewCount(); ++i){
  	  		  List<String> key_list = new ArrayList<String>();
  	  		  //LOG.info(mf.getConPeriod().getValue());
              key_list.add(prefix);
              key_list.add(attr_key);
			  int value = mf.getConPeriodNew(i).getValue();
			  if(value <= 7){
				  key_list.add("一周以内");
			  }
			  else if((value <= 14) && (value >= 8)){
				  key_list.add("半月以内");
			  }
			  else if((value <= 30) && (value >= 15)){
				  key_list.add("一月以内");
			  }
			  else if((value <= 60) && (value >= 31)){
				  key_list.add("两月以内");
			  }
			  else {
				  key_list.add("两月以上");
			  }
	          //key_list.add(String.valueOf(mf.getConPeriodNew(i).getValue()));
	          String key = StringUtils.join(key_list, "/");
			  result.put(key, 1);
	    }
    }

	public static void getConPeriodMarketingFeaturesDistribution(PortraitClass.MarketingFeatures mf,
																 String prefix,
																 Map<String, Integer> result){
		//String attr_key = "mf_cpn";
		String attr_key ="消费周期";
		//key map
//		  Map<String, String> valMap = new HashMap<String, String>();
//		  valMap.put("1", "1周以内");
//		  valMap.put("2", "2周以内");
//		  valMap.put("3","1");
//		  valMap.put("4","4周以内");
//		  valMap.put("5", "一周以内");
		//LOG.info(attr_key);
		if(mf.hasConPeriods()){
			List<String> key_list = new ArrayList<String>();
			//LOG.info(mf.getConPeriod().getValue());
			key_list.add(prefix);
			key_list.add(attr_key);
			int value = mf.getConPeriods().getValue();
			if(value <= 7){
				key_list.add("一周以内");
			}
			else if((value <= 14) && (value >= 8)){
				key_list.add("半月以内");
			}
			else if((value <= 30) && (value >= 15)){
				key_list.add("一月以内");
			}
			else if((value <= 60) && (value >= 31)){
				key_list.add("两月以内");
			}
			else {
				key_list.add("两月以上");
			}
			//key_list.add(String.valueOf(mf.getConPeriodNew(i).getValue()));
			String key = StringUtils.join(key_list, "/");
			result.put(key, 1);
		}
	}
	
    /*
     *  get the Distribution of DemographicInfo
     *  @param: dg_info, the object of DemographicInfo
     *  @param: result, save the results of the distribution of DemographicInfo
     *  @param: prefix, the prefix string of the Fifth category(/消费能力)
     *  @return: void
     */
    public static void getConCapacityFeaturesDistribution(UserProfile2.MarketingFeatures mf,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
  	    String attr_key = "消费能力";
		//key map
//		Map<String, String> valMap = new HashMap<String, String>();
//		valMap.put("3","高");
//		valMap.put("2","中");
//		valMap.put("1","低");

		for(int i=0;i<mf.getConCapacityNewCount();i++){
  	  		List<String> key_list = new ArrayList<String>();
  	  		key_list.add(prefix);
  	  		key_list.add(attr_key);
			int value = mf.getConCapacityNew(i).getValue();
			if(value == 1){
				key_list.add("低");
			}
			else if(value == 2){
				key_list.add("中");
			}
			else{
				key_list.add("高");
			}

			String key = StringUtils.join(key_list, "/");
			result.put(key, 1);
  	  	  }
  	  }

	public static void getConCapacityFeaturesDistribution(PortraitClass.MarketingFeatures mf,
														  String prefix,
														  Map<String, Integer> result){
		String attr_key = "消费能力";
		//key map
//		Map<String, String> valMap = new HashMap<String, String>();
//		valMap.put("3","高");
//		valMap.put("2","中");
//		valMap.put("1","低");

		if(mf.hasConCapacity()){
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			key_list.add(attr_key);
			int value = mf.getConCapacity().getValue();

			if(value == 1){
				key_list.add("低");
			}
			else if(value == 2){
				key_list.add("中");
			}
			else{
				key_list.add("高");
			}

			String key = StringUtils.join(key_list, "/");
			result.put(key, 1);
		}
	}

    
    
    /*
     *  get the Distribution of DemographicInfo
     *  @param: dg_info, the object of DemographicInfo
     *  @param: result, save the results of the distribution of DemographicInfo
     *  @param: prefix, the prefix string of the Fifth category(/价格敏感度)
     *  @return: void
     */
    public static void getPriceSensitivityFeaturesDistribution(UserProfile2.MarketingFeatures mf,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
  	      //String attr_key = "mf_cpn";
  	      String attr_key ="价格敏感度";
  	      //LOG.info(attr_key);
  	      //LOG.info(mf.getConPeriodNewCount());

  	  	  for(int i=0;i<mf.getPriceSensitivityNewCount();++i){
  	  		List<String> key_list = new ArrayList<String>();
              key_list.add(prefix);
              key_list.add(attr_key);
              //LOG.info(mf.getConPeriodNew(i).getValue());
			  double value  = mf.getPriceSensitivityNew(i).getValue();
			  if(value <= 0.3){
				  key_list.add("低");
			  } else if ((value <= 0.45) && (value > 0.3)){
				  key_list.add("中低");
			  }
			  else if((value <= 0.5) && (value > 0.45)){
				  key_list.add("中");
			  }
			  else if((value <= 0.7) && (value > 0.5)){
				  key_list.add("中高");
			  }
			  else {
				  key_list.add("高");
			  }
	          //key_list.add(String.valueOf(mf.getPriceSensitivityNew(i).getValue()));
	          //LOG.info(mf.getConPeriodNew(i).getValue());
	          String key = StringUtils.join(key_list, "/");
			  result.put(key, 1);
	      }
    }

	public static void getPriceSensitivityFeaturesDistribution(PortraitClass.MarketingFeatures mf,
															   String prefix,
															   Map<String, Integer> result){
		//String attr_key = "mf_cpn";
		String attr_key ="价格敏感度";
		//LOG.info(attr_key);
		//LOG.info(mf.getConPeriodNewCount());

		if(mf.hasPriceSensitive()){
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			key_list.add(attr_key);
			//LOG.info(mf.getConPeriodNew(i).getValue());
			double value  = mf.getPriceSensitive().getValue();
			if(value <= 0.3){
				key_list.add("低");
			} else if ((value <= 0.45) && (value > 0.3)){
				key_list.add("中低");
			}
			else if((value <= 0.5) && (value > 0.45)){
				key_list.add("中");
			}
			else if((value <= 0.7) && (value > 0.5)){
				key_list.add("中高");
			}
			else {
				key_list.add("高");
			}
			//key_list.add(String.valueOf(mf.getPriceSensitivityNew(i).getValue()));
			//LOG.info(mf.getConPeriodNew(i).getValue());
			String key = StringUtils.join(key_list, "/");
			result.put(key, 1);
		}
	}
    
    

}
