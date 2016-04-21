package cn.bfd.tools;

import cn.bfd.protobuf.PortraitClass;
import cn.bfd.protobuf.UserProfile2;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GetDemographicInfoAttribute {
	private static Logger LOG = Logger.getLogger(GetDemographicInfoAttribute.class);

	public static void getFirstClassDistribution(UserProfile2.UserProfile up,
												 String prefix,
												 Map<String, Integer> result){
		if(up.hasDgInfo()){
			result.put(prefix, 1);
		}
	}

	public static void getFirstClassDistribution(Map<String, Object> objMap,
												 String prefix,
												 Map<String, Integer> result){
		if(objMap.containsKey("demographic")){
			result.put(prefix, 1);
		}
	}

	public static void getSecondClassDistribution(Map<String, Object> objMap,
												  String prefix,
												  Map<String, Integer> result){
		if(objMap.containsKey("demographic")){
			PortraitClass.DemographicInfo dgInfo = (PortraitClass.DemographicInfo)objMap.get("demographic");
			if(objMap == null){
				return;
			}
			if((dgInfo.hasPreferGender()) || (dgInfo.getPreferAgeCount() != 0)){
				result.put(prefix + "/互联网", 1);
			}
			if((dgInfo.hasAge()) || (dgInfo.hasGender())){
				result.put(prefix + "/自然属性", 1);
			}

			if(dgInfo.getAreasCount() != 0){
				result.put(prefix + "/地区地域",1);
			}
		}
	}

	public static void getSecondClassDistribution(UserProfile2.UserProfile up,
												 String prefix,
												 Map<String, Integer> result){
		if(up.hasDgInfo()){
			UserProfile2.DemographicInfo dgInfo = up.getDgInfo();
			if((dgInfo.hasAge()) || (dgInfo.getSexCount() != 0)){
				result.put(prefix + "/互联网", 1);
			}
			if((dgInfo.hasBioAge()) || (dgInfo.hasBioGender())){
				result.put(prefix + "/自然属性", 1);
			}

			if(dgInfo.getCityCount() != 0){
				result.put(prefix + "/地区地域",1);
			}
		}
	}
	/*
     *  get the Distribution of DemographicInfo
     *  @param: dg_info, the object of DemographicInfo
     *  @param: result, save the results of the distribution of DemographicInfo
     *  @param: prefix, the prefix string of the Fifth category(/人口属性/互联网/性别)
     *  @return: void
     */
     public static void getSexDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
             											  String prefix,
                                                          Map<String, String> keyMap,
                                                          Map<String, Integer> result){ 
    	 String attr_key = "net_sex";
    	// LOG.info(keyMap.get(attr_key));
    	 for(int i = 0; i < dg_info.getSexCount(); ++i){
             //LOG.info(dg_info.getSex(i).getValue());
             List<String> key_list = new ArrayList<String>();
             key_list.add(prefix);
             if(keyMap.containsKey(attr_key)){
            	 key_list.add(keyMap.get(attr_key));
                 key_list.add(dg_info.getSex(i).getValue());
                 String key = StringUtils.join(key_list, "/");
                 LOG.info(key);
                 result.put(key, 1);
             }
             
         }
     }

	public static void getSexDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
														 String prefix,
														 Map<String, String> keyMap,
														 Map<String, Integer> result){
		String attr_key = "net_sex";
		if(dg_info.hasPreferGender()){
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				//对于性别为中的脏数据进行过滤
				if("中".equals(dg_info.getPreferGender().hasValue())){
					return;
				}
				key_list.add(dg_info.getPreferGender().getValue());
				String key = StringUtils.join(key_list, "/");
				result.put(key, 1);
			}

		}
	}

     
     
     
    
      
      /*
       *  get the Distribution of DemographicInfo
       *  @param: dg_info, the object of DemographicInfo
       *  @param: result, save the results of the distribution of DemographicInfo
       *  @param: prefix, the prefix string of the Fifth category(/人口属性/互联网/年龄段)
       *  @return: void
       */
      public static void getAgesDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
																String prefix,
																Map<String, String> keyMap,
																Map<String, String> ageMap,
																Map<String, Integer> result){ 
    	  for(int i = 0; i < dg_info.getAgesCount(); ++i){
              String attr_key = "net_age";
              //LOG.info(keyMap.get(attr_key));
              List<String> key_list = new ArrayList<String>();
              key_list.add(prefix);
             // LOG.info(dg_info.getAges(i).getValue());
              if(keyMap.containsKey(attr_key)){
	              key_list.add(keyMap.get(attr_key));
	              //key_list.add(attr_key);
	              key_list.add(ageMap.get(dg_info.getAges(i).getValue()));
	              //LOG.info(dg_info.getAges(i).getValue());
	              String key = StringUtils.join(key_list, "/");
	              LOG.info(key);
	              result.put(key, 1);
              }
          }
      }

	public static void getAgesDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
														  String prefix,
														  Map<String, String> keyMap,
														  Map<String, String> ageMap,
														  Map<String, Integer> result){
		for(int i = 0; i < dg_info.getPreferAgeCount(); ++i){
			String attr_key = "net_age";
			//LOG.info(keyMap.get(attr_key));
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			// LOG.info(dg_info.getAges(i).getValue());
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				//key_list.add(attr_key);
				key_list.add(ageMap.get(dg_info.getPreferAge(i).getValue()));
				//LOG.info(dg_info.getAges(i).getValue());
				String key = StringUtils.join(key_list, "/");
				LOG.info(key);
				result.put(key, 1);
			}
		}
	}
      
      /*
       *  get the Distribution of DemographicInfo
       *  @param: dg_info, the object of DemographicInfo
       *  @param: result, save the results of the distribution of DemographicInfo
       *  @param: prefix, the prefix string of the Fifth category(/人口属性/地区地域)
       *  @return: void
       */
       public static void getCityDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
               											    String prefix,
                                                            Map<String, String> keyMap,
                                                            Map<String, String> cityMap,
                                                            Map<String, Integer> result){
    	  // LOG.info("进入城市");
      	 for(int i = 0; i < dg_info.getCityCount(); ++i){
			 List<String> key_prov_list = new ArrayList<String>();
			 List<String> key_city_list = new ArrayList<String>();
			 List<String> key_city_level = new ArrayList<String>();

			 key_prov_list.add(prefix);
			 key_city_list.add(prefix);
			 key_city_level.add(prefix);





			 String attr_prov_key = "city";
			 String attr_city_key ="city_type";

   		  //set the prefix of  the province attribute
   		  if(keyMap.containsKey(attr_prov_key)){
   			  key_prov_list.add(keyMap.get(attr_prov_key));
   			  key_prov_list.add("省");
   			  LOG.info(keyMap.get(attr_prov_key));
   		  }

   		  //set the prefix of the city attribute
   		  if(keyMap.containsKey(attr_city_key)){
   			  key_city_list.add(keyMap.get(attr_prov_key));
   			  key_city_list.add("市");
   			LOG.info(keyMap.get(attr_city_key));
   		  }
      		// LOG.info(dg_info.getCity(i).getValue());
 			 String[] ProvAndCity = dg_info.getCity(i).getValue().split("\\$");
 			 if(ProvAndCity.length != 2){
 				 continue;
 			 }
 		//	LOG.info(ProvAndCity[0]);
 			 if(key_prov_list.size() > 1){
 					 key_prov_list.add(ProvAndCity[0]);
 					 String key = StringUtils.join(key_prov_list, "/");
 					 result.put(key, 1);
 			 }
 		//	LOG.info(ProvAndCity[0]);
 			 if(key_city_list.size() > 1){
 					 key_city_list.add(ProvAndCity[1]);
 					 String keyCity = StringUtils.join(key_city_list, "/");
 					 result.put(keyCity, 1);
 					if(cityMap.containsKey(ProvAndCity[1])){
 						 key_city_level.add(keyMap.get(attr_city_key));
						 key_city_level.add(cityMap.get(ProvAndCity[1]));
						 LOG.info(cityMap.get(ProvAndCity[1]));
	 					 String keyLevel = StringUtils.join(key_city_level, "/");
	 					 result.put(keyLevel,1);

					 }
 				 }
           }
      	// LOG.info("离开城市");
       }

	public static void getCityDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
														  String prefix,
														  Map<String, String> keyMap,
														  Map<String, String> cityLevelMap,
														  Map<String, Integer> result){

		for(int i = 0; i < dg_info.getAreasCount(); ++i){
			PortraitClass.AreaInfo areaInfo = dg_info.getAreas(i);
			List<String> key_prov_list = new ArrayList<String>();
			List<String> key_city_list = new ArrayList<String>();
			List<String> key_city_level_list = new ArrayList<String>();
			List<String> key_country_list = new ArrayList<String>();
			List<String> key_district_list = new ArrayList<String>();

			key_prov_list.add(prefix);
			key_city_list.add(prefix);
			key_city_level_list.add(prefix);
			key_country_list.add(prefix);
			key_district_list.add(prefix);

			key_prov_list.add("地区地域");
			key_city_list.add("地区地域");
			key_city_level_list.add("地区地域");
			key_country_list.add("地区地域");
			key_district_list.add("地区地域");

			String attr_key = "locations";

			//set the prefix of the province attribute
			if(!keyMap.containsKey(attr_key)){
				LOG.error("locations is not in keyMap");
				return;
			}

			key_prov_list.add("省");
			key_city_list.add("市");
			key_country_list.add("国家");
			key_district_list.add("区县");
			key_city_level_list.add("城市等级");


			if(areaInfo.hasProvince()){
				key_prov_list.add(areaInfo.getProvince());
				String key = StringUtils.join(key_prov_list, "/");
				result.put(key, 1);
			}

			if(areaInfo.hasCity()){
				String city = areaInfo.getCity();
				key_city_list.add(city);
				String key = StringUtils.join(key_city_list, "/");
				result.put(key, 1);
				//add city_level
				if(cityLevelMap.containsKey(city)){

					key_city_level_list.add(cityLevelMap.get(city)+"线城市");
					key = StringUtils.join(key_city_level_list, "/");
					result.put(key, 1);
				}
				else{
					LOG.warn(city + " does not exists in cityLevelMap.");
				}
			}

			if(areaInfo.hasCountry()){
				key_country_list.add(areaInfo.getCountry());
				String key = StringUtils.join(key_country_list, "/");
				result.put(key, 1);
			}

			if(areaInfo.hasDistrict()){
				key_district_list.add(areaInfo.getDistrict());
				String key = StringUtils.join(key_district_list, "/");
				result.put(key, 1);
			}

		}
	}

      /*
       *  get the Distribution of DemographicInfo
       *  @param: dg_info, the object of DemographicInfo
       *  @param: result, save the results of the distribution of DemographicInfo
       *  @param: prefix, the prefix string of the Fifth category(/人口属性/自然属性/性别)
       *  @return: void
       */
      public static void getBioGenderDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
    	 // LOG.info("进入自然属性性别");
    	  if(dg_info.hasBioGender()){
    		  String attr_key = "bioGender";
              List<String> key_list = new ArrayList<String>();
              key_list.add(prefix);
              //LOG.info(dg_info.getBioGender().getValue());
              if(keyMap.containsKey(attr_key)){
		              key_list.add(keyMap.get(attr_key));
	              //key_list.add(attr_key);
	              key_list.add(dg_info.getBioGender().getValue());
	              //LOG.info(dg_info.getBioGender().getValue());
	              String key = StringUtils.join(key_list, "/");
	              LOG.info(key);
	              result.put(key, 1);
              }
    	  }
    	 // LOG.info("离开自然属性   性别");
      }

	public static void getBioGenderDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
															   String prefix,
															   Map<String, String> keyMap,
															   Map<String, Integer> result){
		// LOG.info("进入自然属性性别");
		if(dg_info.hasGender()){
			String attr_key = "bioGender";
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			//LOG.info(dg_info.getBioGender().getValue());
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				//key_list.add(attr_key);
				key_list.add(dg_info.getGender().getValue());
				//LOG.info(dg_info.getBioGender().getValue());
				String key = StringUtils.join(key_list, "/");
				LOG.info(key);
				result.put(key, 1);
			}
		}
		// LOG.info("离开自然属性   性别");
	}
      
      
      
      /*
       *  get the Distribution of DemographicInfo
       *  @param: dg_info, the object of DemographicInfo
       *  @param: result, save the results of the distribution of DemographicInfo
       *  @param: prefix, the prefix string of the Fifth category(/人口属性/自然属性/年龄段)
       *  @return: void
       */
      public static void getBioAgeDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, String> ageMap,
																	Map<String, Integer> result){
    	 // LOG.info("进入自然属性   年龄段");
    	  if(dg_info.hasBioAge()){
    		  String attr_key = "bioAge";
              List<String> key_list = new ArrayList<String>();
              key_list.add(prefix);
              if(keyMap.containsKey(attr_key)){
		          key_list.add(keyMap.get(attr_key));
	              key_list.add(ageMap.get(dg_info.getBioAge().getValue()));
	              String key = StringUtils.join(key_list, "/");
	              LOG.info(key);
	              result.put(key, 1);
              }
    	  }
    	 // LOG.info("离开自然属性   年龄段");
      }

	public static void getBioAgeDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
															String prefix,
															Map<String, String> keyMap,
															Map<String, String> ageMap,
															Map<String, Integer> result){
		// LOG.info("进入自然属性   年龄段");
		if(dg_info.hasAge()){
			String attr_key = "bioAge";
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				key_list.add(ageMap.get(dg_info.getAge().getValue()));
				String key = StringUtils.join(key_list, "/");
				LOG.info(key);
				result.put(key, 1);
			}
		}
		// LOG.info("离开自然属性   年龄段");
	}

     

      /*
       *  get the Distribution of DemographicInfo
       *  @param: dg_info, the object of DemographicInfo
       *  @param: result, save the results of the distribution of DemographicInfo
       *  @param: prefix, the prefix string of the Fifth category(/人口属性/互联网/婚姻状况)
       *  @return: void
       */
      public static void getMarriedDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
    	 // LOG.info("进入婚姻状况");
    	  if(dg_info.hasMarried()){
    		  String attr_key = "dg_Married";
              List<String> key_list = new ArrayList<String>();
              key_list.add(prefix);
              //key_list.add(attr_key);
                  //do something
              if(keyMap.containsKey(attr_key)){
		          key_list.add(keyMap.get(attr_key));
            	  if(dg_info.getMarried()){
                	  key_list.add("已婚");
                      //LOG.info("已婚");
                      String key = StringUtils.join(key_list, "/");
                      LOG.info(key);
                      result.put(key, 1);
                  } 
              }
    	  }
    	 // LOG.info("离开婚姻状况");
      }

	public static void getMarriedDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
															 String prefix,
															 Map<String, String> keyMap,
															 Map<String, Integer> result){


		if((dg_info.hasMarried()) && (dg_info.getMarried().getValue().equals("1"))){
			String attr_key = "dg_Married";
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				key_list.add("已婚");
				String key = StringUtils.join(key_list, "/");
				result.put(key, 1);
			}
		}

		if((dg_info.hasMarried()) && (dg_info.getMarried().getValue().equals("0"))){
			String attr_key = "dg_Married";
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				key_list.add("未婚");
				String key = StringUtils.join(key_list, "/");
				result.put(key, 1);
			}
		}
		// LOG.info("离开婚姻状况");
	}
      
      /*
       *  get the Distribution of DemographicInfo
       *  @param: dg_info, the object of DemographicInfo
       *  @param: result, save the results of the distribution of DemographicInfo
       *  @param: prefix, the prefix string of the Fifth category(/人口属性/互联网/是否有子女)
       *  @return: void
       */
      public static void getHasBabyDemographicInfoDistribution(UserProfile2.DemographicInfo dg_info,
																	String prefix,
																	Map<String, String> keyMap,
																	Map<String, Integer> result){
    	//  LOG.info("进入是否有子女");
    	  if(dg_info.hasHasBaby()){
    		  String attr_key = "dg_baby";
              List<String> key_list = new ArrayList<String>();
              key_list.add(prefix);
              if(keyMap.containsKey(attr_key)){
		          key_list.add(keyMap.get(attr_key));
	              if(dg_info.getHasBaby()){
	            	  key_list.add("有孩子");
	                  //LOG.info("有孩子");
	                  String key = StringUtils.join(key_list, "/");
	                  LOG.info(key);
	                  result.put(key, 1);
	              }
              }
    	  }
    	 // LOG.info("离开是否有子女");
      }

	public static void getHasBabyDemographicInfoDistribution(PortraitClass.DemographicInfo dg_info,
															 String prefix,
															 Map<String, String> keyMap,
															 Map<String, Integer> result){

		//字符串1代表有孩子
		if((dg_info.hasBaby()) && (dg_info.getBaby().getValue().equals("1"))){
			String attr_key = "dg_baby";
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				key_list.add("有孩子");
				String key = StringUtils.join(key_list, "/");
				result.put(key, 1);
			}
		}

		//字符串0代表无孩子
		if((dg_info.hasBaby()) && (dg_info.getBaby().getValue().equals("0"))){
			String attr_key = "dg_baby";
			List<String> key_list = new ArrayList<String>();
			key_list.add(prefix);
			if(keyMap.containsKey(attr_key)){
				key_list.add(keyMap.get(attr_key));
				key_list.add("无孩子");
				String key = StringUtils.join(key_list, "/");
				result.put(key, 1);
			}
		}
	}
      
      


}
