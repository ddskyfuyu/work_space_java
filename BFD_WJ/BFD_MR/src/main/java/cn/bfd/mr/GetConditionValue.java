package cn.bfd.mr;

import org.codehaus.jettison.json.JSONException;

public class GetConditionValue  {
	public static String getValue (String jsonstr, String condition) {
		String[] conditions = condition.split(",");
		StringBuffer buf = new StringBuffer();
		for (int i=0; i<conditions.length; i++) {
			String[] cons = conditions[i].split("\\.");
			//userprofile.dg_info
			if (cons.length==2 && conditions[i].contains("dg_info")) {
				try {
					buf.append(","+GetValueFromJson.getDginfo(jsonstr, conditions[i]));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			//userprofile.dg_info.internet    userprofile.inter_fts	
			} else if (cons.length==2 && (conditions[i].contains("market_ft") || conditions[i].contains("inter_fts"))
				|| cons.length==3 && (conditions[i].contains("nature") || conditions[i].contains("internet"))) {
				try {
					buf.append(","+GetValueFromJson.getInternet(jsonstr, conditions[i]));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			//userprofile.ec_cate.Cbaifendian	
			} else if (cons.length == 3 && (conditions[i].contains("ec_cate") || conditions[i].contains("media_cate"))) {
				try {
					buf.append(","+GetValueFromJson.getDeepestLabel(jsonstr, conditions[i]));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			//userprofile.ec_cate.Cbaifendian.fc_乳胶�?         userprofile.ec_cate.Cbaifendian.fc_乳胶�?.brands
			} else if (cons.length > 3 && (conditions[i].contains("ec_cate") || conditions[i].contains("media_cate"))) {
				try {
					buf.append(","+GetValueFromJson.getMultiLabel(jsonstr, conditions[i]));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (!conditions[i].contains("(")) {
				try {
					buf.append(","+GetValueFromJson.getLabelValue(jsonstr, conditions[i]));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (conditions[i].startsWith("ec_cate(") || conditions[i].startsWith("media_cate(")){
				String cate = conditions[i].substring(0, conditions[i].indexOf("("));
				int level = Integer.parseInt(
						conditions[i].substring(conditions[i].indexOf("(")+1,
						conditions[i].indexOf(";")));
				int topN = Integer.parseInt(
						conditions[i].substring(conditions[i].indexOf(";")+1, 
						conditions[i].indexOf(")")));
				try {
					buf.append(","+GetValueFromJson.GetTopN(jsonstr, cate, "Cbaifendian", level, topN));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (conditions[i].startsWith("brands(")) {
				int topN = Integer.parseInt(
						conditions[i].substring(conditions[i].indexOf("(")+1, 
						conditions[i].indexOf(")")));
				try {
					buf.append(","+GetValueFromJson.getBrandValue(jsonstr, "Cbaifendian", topN));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (conditions[i].startsWith("fcbtop1(")) {
				int level = Integer.parseInt(
						conditions[i].substring(conditions[i].indexOf("(")+1, 
						conditions[i].indexOf(")")));
				try {
					buf.append(","+GetValueFromJson.GetFirstCateBrandTop1(jsonstr, "ec_cate", "Cbaifendian", level));
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.out.println("nothing to do");
			}
		}
		String result = buf.toString();
		if (!result.equals("")) {
			result = result.substring(1);
		}
		return result;
	}
}
