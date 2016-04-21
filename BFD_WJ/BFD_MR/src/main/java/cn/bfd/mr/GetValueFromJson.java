package cn.bfd.mr;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.*;

public class GetValueFromJson {
	
	static <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> entriesSortedByValues(
			Map<K, V> map) {
		SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<Map.Entry<K, V>>(
				new Comparator<Map.Entry<K, V>>() {
					public int compare(Map.Entry<K, V> e1, Map.Entry<K, V> e2) {
						int res = e1.getValue().compareTo(e2.getValue());
						return res != 0 ? -res : 1;
					}
				});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}
	
	public static String GetTopN (String jsonstr, String cate, String cid, int level, int topN) throws JSONException {
		JSONObject jj = new JSONObject(jsonstr);
		Map<String, Integer> cateweightmap= new TreeMap<String, Integer>();
		Map<String,String> jsonmap = GetJsonMap (jj.getString("userprofile"));
		for (String key : jsonmap.keySet()) {
			//if (key.equals("ec_cate") || key.equals("media_cate")) {
			if (key.equals(cate)) {
				Map<String,String> jsonmap_cate = GetJsonMap(jsonmap.get(key));
				for (String key_cate : jsonmap_cate.keySet()) {
					if (key_cate.equals(cid)) {
						if (level >= 1) {
							Map<String,String> jsonmap_1 = GetJsonMap (jsonmap_cate.get(key_cate));
							for (String key_1 : jsonmap_1.keySet()) {
								if (level == 1) {
									JSONArray ja = new JSONArray(jsonmap_1.get(key_1));
									for (int i=0; i < ja.length(); i++) {
										JSONObject jo = new JSONObject(ja.getString(i));
										if (jo.has("cate_weight")) {
											String res = key_1.split("_")[1];
											cateweightmap.put(res, jo.getInt("cate_weight"));
										}
									}
								}
								if (key_1.startsWith("fc_") && level >= 2) {
									Map<String,String> jsonmap_2 = GetJsonMap (jsonmap_1.get(key_1));
									for (String key_2 : jsonmap_2.keySet()) {
										if (level == 2 && jsonmap_2.get(key_2).startsWith("[")) {
											JSONArray ja = new JSONArray(jsonmap_2.get(key_2));
											for (int i=0; i < ja.length(); i++) {
												JSONObject jo = new JSONObject(ja.getString(i));
												if (jo.has("cate_weight")) {
													String res = key_1.split("_")[1] +"$"+key_2.split("_")[1];
													cateweightmap.put(res, jo.getInt("cate_weight"));
												}
											}
										}
										if (key_2.startsWith("fc_") && level >= 3) {
											Map<String,String> jsonmap_3 = GetJsonMap (jsonmap_2.get(key_2));
											for (String key_3 : jsonmap_3.keySet()) {
												if (level == 3 && jsonmap_3.get(key_3).startsWith("[")) {
													JSONArray ja = new JSONArray(jsonmap_3.get(key_3));
													for (int i=0; i < ja.length(); i++) {
														JSONObject jo = new JSONObject(ja.getString(i));
														if (jo.has("cate_weight")) {
															String res = key_1.split("_")[1] +"$"+key_2.split("_")[1] +"$"+key_3.split("_")[1];
															cateweightmap.put(res, jo.getInt("cate_weight"));
														}
													}
												}
												if (key_3.startsWith("fc_") && level >= 4) {
													Map<String,String> jsonmap_4 = GetJsonMap (jsonmap_3.get(key_3));
													for (String key_4 : jsonmap_4.keySet()) {
														if (level == 4 && jsonmap_4.get(key_4).startsWith("[")) {
															JSONArray ja = new JSONArray(jsonmap_4.get(key_4));
															for (int i=0; i < ja.length(); i++) {
																JSONObject jo = new JSONObject(ja.getString(i));
																if (jo.has("cate_weight")) {
																	String res = key_1.split("_")[1] +"$"+key_2.split("_")[1] +"$"
																			+key_3.split("_")[1] +"$"+key_4.split("_")[1];
																	cateweightmap.put(res, jo.getInt("cate_weight"));
																}
															}
														}
														if (key_4.startsWith("fc_") && level >= 5) {
															Map<String,String> jsonmap_5 = GetJsonMap (jsonmap_4.get(key_4));
															for (String key_5 : jsonmap_5.keySet()) {
																if (level == 5 && jsonmap_5.get(key_5).startsWith("[")) {
																	JSONArray ja = new JSONArray(jsonmap_5.get(key_5));
																	for (int i=0; i < ja.length(); i++) {
																		JSONObject jo = new JSONObject(ja.getString(i));
																		if (jo.has("cate_weight")) {
																			String res = key_1.split("_")[1] +"$"+key_2.split("_")[1] +"$"
																					+key_3.split("_")[1] +"$"+key_4.split("_")[1] +"$"+key_5.split("_")[1];
																			cateweightmap.put(res, jo.getInt("cate_weight"));
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}//for
		StringBuffer cateBuffer = new StringBuffer("");
		int currentNum = 1;
		for (Map.Entry<String, Integer> iter : entriesSortedByValues(cateweightmap)) {
			if (currentNum == 1) {
				cateBuffer.append(iter.getKey() + ">" + iter.getValue());
			} else {
				cateBuffer.append("|" + iter.getKey() + ">" + iter.getValue());
			}
			if (++currentNum > topN) {
				break;
			}
		}
		String cateStr = cateBuffer.toString();
		return cateStr.equals("")?"0":cateStr;
	}
	
//	public static String GetFirstCateBrandTop1 (String jsonstr, String userprofile, int level) throws JSONException  {
//		String res = "";
//		JSONObject jj = new JSONObject(jsonstr);
//		Map<String,String> jsonmap = GetJsonMap (jj.getString(userprofile));
//		for (String key : jsonmap.keySet()) {
//			if (key.equals("ec_cate") || key.equals("media_cate")) {
//				Map<String,String> jsonmap_cate = GetJsonMap (jsonmap.get(key));
//				for (String key_cate : jsonmap_cate.keySet()) {
//					if (level >= 1) {
//						Map<String,String> jsonmap_1 = GetJsonMap (jsonmap_cate.get(key_cate));
//						for (String key_1 : jsonmap_1.keySet()) {
//							if (level == 1 && jsonmap_1.get(key_1).startsWith("[")) {
//								JSONArray ja_1 = new JSONArray(jsonmap_1.get(key_1));
//								for (int i=0; i<ja_1.length(); i++) {
//									JSONObject jo_1 = new JSONObject(ja_1.getString(i));
//									if (jo_1.has("brands")) {
//										JSONArray ja_br_1 = new JSONArray(jo_1.getString("brands"));
//										if (ja_br_1.length()>0) {
//											JSONObject jo_br_1 = new JSONObject(ja_br_1.getString(0));
//											res = key_1.split("_")[1]+"|"+jo_br_1.getString("value");
//										} else {
//											res = key_1.split("_")[1]+"|0";
//										}
//									}
//								}
//							}
//							if (key_1.startsWith("fc_") && level >= 2) {
//								Map<String,String> jsonmap_2 = GetJsonMap (jsonmap_1.get(key_1));
//								for (String key_2 : jsonmap_2.keySet()) {
//									if (level == 2 && jsonmap_2.get(key_2).startsWith("[")) {
//										JSONArray ja_2 = new JSONArray(jsonmap_2.get(key_2));
//										for (int i=0; i<ja_2.length(); i++) {
//											JSONObject jo_2 = new JSONObject(ja_2.getString(i));
//											if (jo_2.has("brands")) {
//												JSONArray ja_br_2 = new JSONArray(jo_2.getString("brands"));
//												if (ja_br_2.length()>0) {
//													JSONObject jo_br_2 = new JSONObject(ja_br_2.getString(0));
//													res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"|"+jo_br_2.getString("value");
//												} else {
//													res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"|0";
//												}
//											}
//										}
//									}
//									if (key_2.startsWith("fc_") && level >= 3) {
//										Map<String,String> jsonmap_3 = GetJsonMap (jsonmap_2.get(key_2));
//										for (String key_3 : jsonmap_3.keySet()) {
//											if (level == 3 && jsonmap_3.get(key_3).startsWith("[")) {
//												JSONArray ja_3 = new JSONArray(jsonmap_3.get(key_3));
//												for (int i=0; i<ja_3.length(); i++) {
//													JSONObject jo_3 = new JSONObject(ja_3.getString(i));
//													if (jo_3.has("brands")) {
//														JSONArray ja_br_3 = new JSONArray(jo_3.getString("brands"));
//														if (ja_br_3.length()>0) {
//															JSONObject jo_br_3 = new JSONObject(ja_br_3.getString(0));
//															res = key_1.split("_")[1]+"$"+key_2.split("_")[1]
//																	+"$"+key_3.split("_")[1]+"|"+jo_br_3.getString("value");
//														} else {
//															res = key_1.split("_")[1]+"$"+key_2.split("_")[1]
//																	+"$"+key_3.split("_")[1]+"|0";
//														}
//													}
//												}
//											}
//											if (key_3.startsWith("fc_") && level >= 4) {
//												Map<String,String> jsonmap_4 = GetJsonMap (jsonmap_3.get(key_3));
//												for (String key_4 : jsonmap_4.keySet()) {
//												if (level == 4 && jsonmap_4.get(key_4).startsWith("[")) {
//														JSONArray ja_4 = new JSONArray(jsonmap_4.get(key_4));
//														for (int i=0; i<ja_4.length(); i++) {
//															JSONObject jo_4 = new JSONObject(ja_4.getString(i));
//															if (jo_4.has("brands")) {
//																JSONArray ja_br_4 = new JSONArray(jo_4.getString("brands"));
//																if (ja_br_4.length()>0) {
//																	JSONObject jo_br_4 = new JSONObject(ja_br_4.getString(0));
//																	res = key_1.split("_")[1]+"$"+key_2.split("_")[1]
//																			+"$"+key_3.split("_")[1]+"$"+key_4.split("_")[1]+"|"+jo_br_4.getString("value");
//																} else {
//																	res = key_1.split("_")[1]+"$"+key_2.split("_")[1]
//																			+"$"+key_3.split("_")[1]+"$"+key_4.split("_")[1]+"|0";
//																}
//															}
//														}
//													}
//													if (key_4.startsWith("fc_") && level >= 5) {
//														Map<String,String> jsonmap_5 = GetJsonMap (jsonmap_4.get(key_4));
//														for (String key_5 : jsonmap_5.keySet()) {
//														if (level == 5 && jsonmap_5.get(key_5).startsWith("[")) {
//																JSONArray ja_5 = new JSONArray(jsonmap_5.get(key_5));
//																for (int i=0; i<ja_5.length(); i++) {
//																	JSONObject jo_5 = new JSONObject(ja_5.getString(i));
//																	if (jo_5.has("brands")) {
//																		JSONArray ja_br_5 = new JSONArray(jo_5.getString("brands"));
//																		if (ja_br_5.length()>0) {
//																			JSONObject jo_br_5 = new JSONObject(ja_br_5.getString(0));
//																			res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]
//																					+"$"+key_4.split("_")[1]+"$"+key_5.split("_")[1]+"|"+jo_br_5.getString("value");
//																		} else {
//																			res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]
//																					+"$"+key_4.split("_")[1]+"$"+key_5.split("_")[1]+"|0";
//																		}
//																	}
//																}
//															}
//														}
//													}
//												}
//											}
//										}
//									}
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//		return res;
//	}
	
	public static String getBrandValue (String jsonstr, String cid, int topN) throws JSONException {
		Map<String, Integer> brandMap = new TreeMap<String, Integer>();
		JSONObject jsonobj = new JSONObject(jsonstr);
		Map<String,String> jsonmap = GetJsonMap (jsonobj.getString("userprofile"));
		for (String key : jsonmap.keySet()) {
			if (key.equals("media_cate") || key.equals("ec_cate")) {
				Map<String, String> jsonmap_cate = GetJsonMap(jsonmap.get(key));
				for (String key_cate : jsonmap_cate.keySet()) {
					if (key_cate.equals(cid)) {
						Map<String,String> jsonmap_cid = GetJsonMap(jsonmap_cate.get(key_cate));
						for (String key_cid : jsonmap_cid.keySet()) {
							if (key_cid.equals("indus_brands")) {
								JSONArray ja = new JSONArray(jsonmap_cid.get(key_cid));
								for (int i=0; i<ja.length(); i++) {
									JSONObject jo = new JSONObject(ja.getString(i));
									brandMap.put(jo.getString("value"), jo.getInt("weight"));
								}
							}
						}
					}
				}
			}
		}
		StringBuffer brandBuffer = new StringBuffer();
		int currentNum = 1;
		for (Map.Entry<String, Integer> iter : entriesSortedByValues(brandMap)) {
			if (currentNum == 1) {
				brandBuffer.append(iter.getKey() + ">" + iter.getValue());
			} else {
				brandBuffer.append("|" + iter.getKey() + ">" + iter.getValue());
			}
			if (++currentNum > topN) {
				break;
			}
		}
		String brandStr = brandBuffer.toString();
		return brandStr.equals("")?"0":brandStr;
	}
	
	public static String getLabelValue (String jsonstr, String label) throws JSONException {
		String label_value = ""; 
		int i = 0;
		String[] labels = label.split("\\.");
		String str_return = jsonstr;
		while (i<labels.length && !str_return.equals("")) {
			str_return =  findLabel(str_return,labels[i]);
			i++;
		}
		if (str_return.startsWith("[")) {
			JSONArray jsonarray = new JSONArray(str_return);
			for (int j=0; j<jsonarray.length(); j++) {
				JSONObject jsonobject = new JSONObject(jsonarray.getString(j));
				if (jsonobject.has("value")) {
					label_value = jsonobject.getString("value");
				}
			}
			
		} else if (str_return.startsWith("{")) {
			JSONObject jsonobject = new JSONObject(str_return);
			if (jsonobject.has("value")) {
				label_value = jsonobject.getString("value");
			}
		} else {
			label_value = str_return;
		}
		return label_value.equals("")?"0":label_value;
	}
	
	public static String GetFirstCateBrandTop1 (String jsonstr, String cate, String cid, int level) throws JSONException  {
		String res = "";
		JSONObject jj = new JSONObject(jsonstr);
		Map<String,String> jsonmap = GetJsonMap (jj.getString("userprofile"));
		for (String key : jsonmap.keySet()) {
			if (key.equals(cate)) {	
				JSONArray jsonarr_cid = new JSONArray(jsonmap.get(key));
				for (int i_cid=0; i_cid<jsonarr_cid.length(); i_cid++) {
					JSONObject jsonobj_cid = jsonarr_cid.getJSONObject(i_cid);
					if (jsonobj_cid.has(cid)) {
						JSONArray jsonarr_1 = new JSONArray(jsonobj_cid.getString(cid));
						int flag_0 = 0;
						if (jsonarr_1.length()>0) {
							if (level>=1) {
								for (int i_1=0; i_1<jsonarr_1.length() && flag_0 == 0; i_1++) {
								JSONObject josnobj_1 = jsonarr_1.getJSONObject(i_1);
								Iterator it_1 = josnobj_1.keys();
								String key_1 = "";
								int flag_1 = 0;
								while (it_1.hasNext() && flag_1 ==0) {
									key_1 = (String) it_1.next();
									if (key_1.startsWith("fc_")) {
										flag_0 = 1;
										flag_1 = 1;
										JSONArray jsonarr_2 = new JSONArray(josnobj_1.getString(key_1));
										if (jsonarr_2.length()>0) {
											if (level>=2) {
												JSONObject josnobj_2 = jsonarr_2.getJSONObject(0);
												Iterator it_2 = josnobj_2.keys();
												String key_2 = "";
												int flag_2 = 0;
												while (it_2.hasNext() && flag_2 ==0) {
													key_2 = (String) it_2.next();
													flag_2 = 1;
												}
												if (key_2.startsWith("fc_")) {
													JSONArray jsonarr_3 = new JSONArray(josnobj_2.getString(key_2));
													if (jsonarr_3.length()>0) {
														if (level>=3) {
															JSONObject josnobj_3 = jsonarr_3.getJSONObject(0);
															Iterator it_3 = josnobj_3.keys();
															String key_3 = "";
															int flag_3 = 0;
															while (it_3.hasNext() && flag_3 ==0) {
																key_3 = (String) it_3.next();
																flag_3 = 1;
															}
															if (key_3.startsWith("fc_")) {
																JSONArray jsonarr_4 = new JSONArray(josnobj_3.getString(key_3));
																if (jsonarr_4.length()>0) {
																	if (level>=4) {
																		JSONObject josnobj_4 = jsonarr_4.getJSONObject(0);
																		Iterator it_4 = josnobj_4.keys();
																		String key_4 = "";
																		int flag_4 = 0;
																		while (it_4.hasNext() && flag_4 ==0) {
																			key_4 = (String) it_4.next();
																			flag_4 = 1;
																		}
																		if (key_4.startsWith("fc_")) {
																			JSONArray jsonarr_5 = new JSONArray(josnobj_4.getString(key_4));
																			if (jsonarr_5.length()>0) {
																				if (level>=5) {
																					JSONObject josnobj_5 = jsonarr_5.getJSONObject(0);
																					Iterator it_5 = josnobj_5.keys();
																					String key_5 = "";
																					int flag_5 = 0;
																					while (it_5.hasNext() && flag_5 ==0) {
																						key_5 = (String) it_5.next();
																						flag_5 = 1;
																					}
																					if (level==5) {
																						if (key_5.startsWith("fc_")) {
																							if (josnobj_5.getString(key_5).startsWith("[")) {
																								Map<String,String> map_5 = GetJsonMap(josnobj_5.getString(key_5)) ;
																								for (String mapkey_5 : map_5.keySet()) {
																									if (mapkey_5.equals("brands")) {
																										JSONArray ja_br_5 = new JSONArray(map_5.get(mapkey_5));
																										if (ja_br_5.length()>0) {
																											JSONObject jo_br_5 = ja_br_5.getJSONObject(0);
																											res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]
																													+"$"+key_4.split("_")[1]+"$"+key_5.split("_")[1]+"|"+jo_br_5.getString("value");
																										} else {
																											res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]
																													+"$"+key_4.split("_")[1]+"$"+key_5.split("_")[1]+"|0";
																										}
																									}
																								}
																							}
																						}
																					}
																				}
																			}
																		}
																		if (level==4) {
																			if (josnobj_4.getString(key_4).startsWith("[")) {
																				Map<String,String> map_4 = GetJsonMap(josnobj_4.getString(key_4)) ;
																				for (String mapkey_4 : map_4.keySet()) {
																					if (mapkey_4.equals("brands")) {
																						JSONArray ja_br_4 = new JSONArray(map_4.get(mapkey_4));
																						if (ja_br_4.length()>0) {
																							JSONObject jo_br_4 = ja_br_4.getJSONObject(0);
																							res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]
																									+"$"+key_4.split("_")[1]+"|"+jo_br_4.getString("value");
																						} else {
																							res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]
																									+"$"+key_4.split("_")[1]+"|0";
																						}
																					}
																				}
																			}
																		}
																	}
																	
																}
															}
															if (level==3) {
																if (josnobj_3.getString(key_3).startsWith("[")) {
																	Map<String,String> map_3 = GetJsonMap(josnobj_3.getString(key_3)) ;
																	for (String mapkey_3 : map_3.keySet()) {
																		if (mapkey_3.equals("brands")) {
																			JSONArray ja_br_3 = new JSONArray(map_3.get(mapkey_3));
																			if (ja_br_3.length()>0) {
																				JSONObject jo_br_3 = ja_br_3.getJSONObject(0);
																				res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]+"|"+jo_br_3.getString("value");
																			} else {
																				res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"$"+key_3.split("_")[1]+"|0";
																			}
																		}
																	}
																}
															}
														}
														
													}
												}
												if (level==2) {
													if (josnobj_2.getString(key_2).startsWith("[")) {
														Map<String,String> map_2 = GetJsonMap(josnobj_2.getString(key_2)) ;
														for (String mapkey_2 : map_2.keySet()) {
															if (mapkey_2.equals("brands")) {
																JSONArray ja_br_2 = new JSONArray(map_2.get(mapkey_2));
																if (ja_br_2.length()>0) {
																	JSONObject jo_br_2 = ja_br_2.getJSONObject(0);
																	res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"|"+jo_br_2.getString("value");
																} else {
																	res = key_1.split("_")[1]+"$"+key_2.split("_")[1]+"|0";
																}
															}
														}
													}
												}
											}
										}
									}
									if (level==1) {
										if (josnobj_1.getString(key_1).startsWith("[")) {
											Map<String,String> map_1 = GetJsonMap(josnobj_1.getString(key_1)) ;
											for (String mapkey_1 : map_1.keySet()) {
												if (mapkey_1.equals("brands")) {
													JSONArray ja_br_1 = new JSONArray(map_1.get(mapkey_1));
													if (ja_br_1.length()>0) {
														JSONObject jo_br_1 = ja_br_1.getJSONObject(0);
														res = key_1.split("_")[1]+"|"+jo_br_1.getString("value");
													} else {
														res = key_1.split("_")[1]+"|0";
													}
												}
											}
										}
									}
								}
								}
							}
						}
					}
				}
			}
		}
		return res.equals("")?"0":res;
	}
	
	public static String getDginfo (String jsonstr, String label) throws JSONException {
		String res = "";
		String[] labels = label.split("\\.");
		JSONObject jsonobj = new JSONObject(jsonstr);
		JSONArray jsonarr = new JSONArray(jsonobj.getString(labels[0]));
		for (int i=0; i<jsonarr.length(); i++) {
			JSONObject jo = jsonarr.getJSONObject(i);
			Iterator it = jo.keys();
			while (it.hasNext()) {
				String next = (String) it.next();
				if (next.equals("dg_info")) {
					if (jo.getString(next).startsWith("[")) {
						JSONArray jsonarr_1 = new JSONArray(jo.getString(next));
						for (int j=0; j<jsonarr_1.length(); j++){
							JSONObject jsonobj_1 = jsonarr_1.getJSONObject(j);
							Iterator it_1 = jsonobj_1.keys();
							while (it_1.hasNext()) {
								String next_1 = (String) it_1.next();
								JSONArray jsonarr_2 = new JSONArray(jsonobj_1.getString(next_1));
								for (int k=0; k<jsonarr_2.length(); k++) {
									JSONObject jsonobj_2 = jsonarr_2.getJSONObject(k);
									Iterator it_2 = jsonobj_2.keys();
									while (it_2.hasNext()) {
										String next_2 = (String) it_2.next();
										if (jsonobj_2.getString(next_2).startsWith("[")) {
											JSONArray jsonarr_3 = new JSONArray(jsonobj_2.getString(next_2));
											for (int l=0; l<jsonarr_3.length(); l++) {
												JSONObject jsonobj_3 = jsonarr_3.getJSONObject(l);
												if (jsonobj_3.has("value")) {
													res += "|"+next_2+":"+jsonobj_3.getString("value");
												}
											}
										} else if (jsonobj_2.getString(next_2).startsWith("{")) {
											JSONObject jsonobj_3 = new JSONObject(jsonobj_2.getString(next_2));
											if (jsonobj_3.has("value")) {
												res += "|"+next_2+":"+jsonobj_3.getString("value");
											}
										} else {
											res += "|"+next_2+":"+jsonobj_2.getString(next_2);
										}
									}
								}
							}
						}
					} else if (jo.getString(next).startsWith("{")) {
						JSONObject jsonobj_1 = new JSONObject(jo.getString(next));
						Iterator it_1 = jsonobj_1.keys();
						while (it_1.hasNext()) {
							String next_1 = (String) it_1.next();
							if (jsonobj_1.getString(next_1).startsWith("[")) {
								JSONArray jsonarr_2 = new JSONArray(jsonobj_1.getString(next_1));
								for (int k=0; k<jsonarr_2.length(); k++) {
									JSONObject jsonobj_2 = jsonarr_2.getJSONObject(k);
									Iterator it_2 = jsonobj_2.keys();
									while (it_2.hasNext()) {
										String next_2 = (String) it_2.next();
										JSONArray jsonarr_3 = new JSONArray(jsonobj_2.getString(next_2));
										for (int l=0; l<jsonarr_3.length(); l++) {
											JSONObject jsonobj_3 = jsonarr_3.getJSONObject(l);
											if (jsonobj_3.has("value")) {
												res += "|"+next_2+":"+jsonobj_3.getString("value");
											}
										}
									}
								}
							} else if (jsonobj_1.getString(next_1).startsWith("{")) {
								JSONObject jsonobj_2 = new JSONObject(jsonobj_1.getString(next_1));
								if (jsonobj_2.has("value")) {
									res += "|"+next_1+":"+jsonobj_2.getString("value");
								}
							}
						}
					} else {
						res += "|"+next+":"+jo.getString(next);
					}
				}
			}
		}
		return res.length()>0?res.substring(1):"0";
	}
	
	public static String getInternet (String jsonstr, String label) throws JSONException {
		String res = ""; 
		int i = 0;
		String[] labels = label.split("\\.");
		String str_return = jsonstr;
		while (i<labels.length && !str_return.equals("")) {
			str_return =  find(str_return,labels[i]);
			i++;
		}
		if (str_return.startsWith("[")) {
			JSONArray jsonarr = new JSONArray(str_return);
			for (int j=0; j<jsonarr.length(); j++) {
				JSONObject jsonobj = jsonarr.getJSONObject(j);
				Iterator it = jsonobj.keys();
				while (it.hasNext()) {
					String next = (String) it.next();
					if (jsonobj.getString(next).startsWith("[")) {
						JSONArray jsonarr_1 = new JSONArray(jsonobj.getString(next));
						for (int k=0; k<jsonarr_1.length(); k++) {
							JSONObject jsonobj_1 = jsonarr_1.getJSONObject(k);
							if (jsonobj_1.has("value")) {
								res += "|"+next+":"+jsonobj_1.getString("value");
							}
						}
					} else if (jsonobj.getString(next).startsWith("{")) {
						JSONObject jsonobj_1 = new JSONObject(jsonobj.getString(next));
						if (jsonobj_1.has("value")) {
							res += "|"+next+":"+jsonobj_1.getString("value");
						}
					}
				}
			}
		} else if (str_return.startsWith("{")) {
			JSONObject jsonobj = new JSONObject(str_return);
			Iterator it = jsonobj.keys();
			while (it.hasNext()) {
				String next = (String) it.next();
				if (jsonobj.getString(next).startsWith("[")) {
					JSONArray jsonarr = new JSONArray(jsonobj.getString(next));
					for (int k=0; k<jsonarr.length(); k++) {
						JSONObject jsonobj_1 = jsonarr.getJSONObject(k);
						if (jsonobj_1.has("value")) {
							res += "|"+next+":"+jsonobj_1.getString("value");
						}
					}
				} else if (jsonobj.getString(next).startsWith("{")) {
//					JSONObject jsonobj_1 = new JSONObject(jsonobj.getString(next));
//					if (jsonobj_1.has("value")) {
//						res += "|"+next+":"+jsonobj_1.getString("value");
//					}
					JSONObject jsonobj_1 = new JSONObject(jsonobj.getString(next));
					Iterator iterator = jsonobj_1.keys();
					while (iterator.hasNext()) {
						String next_1 = (String) iterator.next();
						if (jsonobj_1.getString(next_1).startsWith("[")) {
							JSONArray jsonarr_1 = new JSONArray(jsonobj_1.getString(next_1));
							String strarr = "";
							for (int i_1=0; i_1<jsonarr_1.length(); i_1++) {
								JSONObject jsonobj_2 = jsonarr_1.getJSONObject(i_1);
								if (jsonobj_2.has("value")) {
									strarr += "#"+jsonobj_2.getString("value");
								}
							}
							if (strarr.length()>0) {
								strarr = strarr.substring(1);
							}
							res += "|"+next_1+":"+strarr;
						} else {
							res += "|"+next_1+":"+jsonobj_1.getString(next_1);
						}
					}
				}
			}
		}
		return res.length()>0?res.substring(1):"0";
	}
	
	public static String getMultiLabel (String jsonstr, String label) throws JSONException {
		String label_value = ""; 
		Map<String, String> map = new HashMap<String, String>();
		int i = 0;
		String[] labels = label.split("\\.");
		String str_return = jsonstr;
		while (i<labels.length && !str_return.equals("")) {
			str_return =  find(str_return,labels[i]);
			i++;
		}
		if (str_return.startsWith("[") && labels[i-1].startsWith("fc_")) {
			JSONArray jsonarray = new JSONArray(str_return);
			for (int j=0; j<jsonarray.length(); j++) {
				JSONObject jsonobj = new JSONObject(jsonarray.getString(j));
				Iterator it = jsonobj.keys();
				while (it.hasNext()) {
					String next_key = (String) it.next();
					String next_value = jsonobj.getString(next_key); 
					if (next_value.startsWith("[") && !next_key.startsWith("fc_")) {
						JSONArray jsonarray_1 = new JSONArray(next_value);
//						if (jsonarray_1.length()>0) {
//							JSONObject jsonobj_1 = new JSONObject(jsonarray_1.getString(0));
//							if (jsonobj_1.has("value")) {
//								map.put(next_key, jsonobj_1.getString("value"));
//							}
//						}
						String arrayval = "";
						for (int i_1=0; i_1<jsonarray_1.length(); i_1++) {
							JSONObject jsonobj_1 = new JSONObject(jsonarray_1.getString(i_1));
							if (jsonobj_1.has("value")) {
								arrayval += "#" + jsonobj_1.getString("value");
							}
						}
						if (arrayval.length()>0) {
							arrayval = arrayval.substring(1);
						}
						map.put(next_key, arrayval);
					} else if (next_value.startsWith("{") && !next_key.startsWith("fc_")) {
						
					} else if (!next_key.startsWith("fc_")) {
						map.put(next_key, next_value);
					}
				}
			}
			for (String mapkey : map.keySet()) {
				label_value += "|"+mapkey+":"+map.get(mapkey);
			}
			return label_value.length()>0?label_value.substring(1):label_value;
		} else if (str_return.startsWith("[")) {
			JSONArray jsonarray_2 = new JSONArray(str_return);
			for (int l=0; l<jsonarray_2.length(); l++) {
				JSONObject jsonobject_2 = new JSONObject(jsonarray_2.getString(l));
				if (jsonobject_2.has("value")) {
					label_value = jsonobject_2.getString("value");
				}
			}
			return label_value;
		} else if (str_return.startsWith("{")) {
			return "0";
		} else {
			label_value=str_return;
			return label_value.equals("")?"0":label_value;
		}
	}
	
	public static String find(String jsonstr, String label) throws JSONException {
		String str = "";
		if (jsonstr.startsWith("[")) {
			Map<String, String> jsonmap_1 = GetJsonMap(jsonstr);
			for (String key_1 : jsonmap_1.keySet()) {
				if (key_1.equals(label)) {
					str = jsonmap_1.get(key_1);
				}
			}
		} else if (jsonstr.startsWith("{")) {
			JSONObject jsonobj_1 = new JSONObject(jsonstr);
			if (jsonobj_1.has(label)) {
				str = jsonobj_1.getString(label);
			}
		}
		return str;
	}
	
	public static String getDeepestLabel (String jsonstr, String label) throws JSONException {
		String label_value = "";
		ArrayList<String> list = new ArrayList<String>();
		String res = "";
		String[] labels = label.split("\\.");
		JSONObject jsonobj = new JSONObject(jsonstr);
		String str_return = jsonobj.getString(labels[0]);
		
		Map<String,String> map_1 = GetJsonMap(str_return);
		for (String key_1 : map_1.keySet()) {
			if (map_1.get(key_1).startsWith("[") && key_1.equals(labels[1])) {
				Map<String,String> map_2 = GetJsonMap(map_1.get(key_1));
				//labels[2] Cbaifendian
				if (map_2.containsKey(labels[2])) {
					if (map_2.get(labels[2]).startsWith("[")) {
						JSONArray jsonarr_0 = new JSONArray(map_2.get(labels[2]));
						for (int i_0=0; i_0<jsonarr_0.length(); i_0++) {
							JSONObject jsonobj_0 = jsonarr_0.getJSONObject(i_0);
							Iterator it_0 = jsonobj_0.keys();
							while (it_0.hasNext()) {
								String next_0 = (String) it_0.next();
								if (next_0.startsWith("fc_")) {
									JSONArray jsonarr_1 = new JSONArray(jsonobj_0.getString(next_0));
									for (int i_1=0; i_1<jsonarr_1.length(); i_1++) {
										JSONObject jsonobj_1 = jsonarr_1.getJSONObject(i_1);
										if (jsonobj_1.has("brands")) {
											res = next_0.substring(3)+"$"+tomap(jsonobj_0.getString(next_0));
											if (!res.equals("")) {
												list.add(res);
											}
										}
										Iterator it_1 = jsonobj_1.keys();
										while (it_1.hasNext()) {
											String next_1 = (String) it_1.next();
											if (next_1.startsWith("fc_")) {
												JSONArray jsonarr_2 = new JSONArray(jsonobj_1.getString(next_1));
												for (int i_2=0; i_2<jsonarr_2.length(); i_2++) {
													JSONObject jsonobj_2 = jsonarr_2.getJSONObject(i_2);
													if (jsonobj_2.has("brands")) {
														res = next_0.substring(3)+"$"+next_1.substring(3)+"$"
																+tomap(jsonobj_1.getString(next_1));
														if (!res.equals("")) {
															list.add(res);
														}
													}
													Iterator it_2 = jsonobj_2.keys();
													while (it_2.hasNext()) {
														String next_2 = (String) it_2.next();
														if (next_2.startsWith("fc_")) {
															JSONArray jsonarr_3 = new JSONArray(jsonobj_2.getString(next_2));
															for (int i_3=0; i_3<jsonarr_3.length(); i_3++) {
																JSONObject jsonobj_3 = jsonarr_3.getJSONObject(i_3);
																if (jsonobj_3.has("brands")) {
																	res = next_0.substring(3)+"$"+next_1.substring(3)+"$"
																			+next_2.substring(3)+"$"+tomap(jsonobj_2.getString(next_2));
																	if (!res.equals("")) {
																		list.add(res);
																	}
																}
																Iterator it_3 = jsonobj_3.keys();
																while (it_3.hasNext()) {
																	String next_3 = (String) it_3.next();
																	if (next_3.startsWith("fc_")) {
																		JSONArray jsonarr_4 = new JSONArray(jsonobj_3.getString(next_3));
																		for (int i_4=0; i_4<jsonarr_4.length(); i_4++) {
																			JSONObject jsonobj_4 = jsonarr_4.getJSONObject(i_4);
																			if (jsonobj_4.has("brands")) {
																				res = next_0.substring(3)+"$"+next_1.substring(3)+"$"
																						+next_2.substring(3)+"$"+next_3.substring(3)+"$"
																						+tomap(jsonobj_3.getString(next_3));
																				if (!res.equals("")) {
																					list.add(res);
																				}
																			}
																			Iterator it_4 = jsonobj_4.keys();
																			while (it_4.hasNext()) {
																				String next_4 = (String) it_4.next();
																				if (jsonobj_4.getString(next_4).startsWith("[")) {
																					res = next_0.substring(3)+"$"+next_1.substring(3)+"$"
																							+next_2.substring(3)+"$"+next_3.substring(3)+"$"
																							+next_4.substring(3)+"$"+tomap(jsonobj_4.getString(next_4));
																					if (!res.equals("")) {
																						list.add(res);
																					}
																				}
																			}
																		}
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		Iterator iterator = list.iterator();
		while (iterator.hasNext()) {
			label_value += ";"+iterator.next();
		}
		return label_value.length()>0?label_value.substring(1):"0";
	}
	
	private static String tomap(String str) throws JSONException {
		String res = "";
		Map<String,String> map = new HashMap<String, String>();
		JSONArray jsonarr = new JSONArray(str);
		for (int i=0; i<jsonarr.length(); i++) {
			JSONObject jsonobj = jsonarr.getJSONObject(i);
			Iterator it = jsonobj.keys();
			while (it.hasNext()) {
				String next = (String) it.next();
				if (jsonobj.getString(next).startsWith("[")) {
					JSONArray ja = new JSONArray(jsonobj.getString(next));
					String jsonstr = "";
					for (int j=0; j<ja.length(); j++) {
						JSONObject jo = ja.getJSONObject(j);
						if (jo.has("value")) {
							jsonstr += "#"+jo.getString("value");
						}
					}
					if (jsonstr.length()>0) {
						jsonstr = jsonstr.substring(1);
					}
					map.put(next, jsonstr);
				} else {
					map.put(next, jsonobj.getString(next));
				}
			}
		}
		for (String key : map.keySet()) {
			res += "|"+key+":"+map.get(key);
		}
		return res.length()>0?res.substring(1):res;
	}
	
	private static String findLabel(String jsonstr, String label) throws JSONException {
		String str = "0";
		if (jsonstr.startsWith("[")) {
			Map<String, String> jsonmap_1 = GetJsonMap(jsonstr);
			for (String key_1 : jsonmap_1.keySet()) {
				if (key_1.equals(label)) {
					str = jsonmap_1.get(key_1);
				}
			}
		} else if (jsonstr.startsWith("{")) {
			JSONObject jsonobj_1 = new JSONObject(jsonstr);
			if (jsonobj_1.has(label)) {
				str = jsonobj_1.getString(label);
			}
		}
		return str;
	}
	
	private static Map<String,String> GetJsonMap (String jsonstr) throws JSONException {
		Map<String,String> map = new HashMap<String,String>();
		JSONArray ja = new JSONArray(jsonstr); 
		for (int i=0; i<ja.length(); i++) {
			JSONObject jsonobj = new JSONObject(ja.getString(i));
			Iterator it = jsonobj.keys();
			while (it.hasNext()) {
				String next = (String) it.next();
				map.put(next, jsonobj.getString(next));
			}
		}
		return map;
	}
}
