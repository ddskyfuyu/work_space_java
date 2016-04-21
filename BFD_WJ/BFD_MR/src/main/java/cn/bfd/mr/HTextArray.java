package cn.bfd.mr;


import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class HTextArray extends ArrayWritable {
	public HTextArray(){
		super(Text.class);
	}
	
	public HTextArray(String[] strings){
		super(Text.class);
		Text[] texts = new Text[strings.length];
		for(int i = 0; i < strings.length; ++i){
			texts[i] = new Text(strings[i]);
		}
		set(texts);
	}
	
	public HTextArray(List<String> strings){
		super(Text.class);
		Text[] texts = new Text[strings.size()];
		for(int i = 0; i < strings.size(); ++i){
			texts[i] = new Text(strings.get(i));
		}
		set(texts);
	}

}
