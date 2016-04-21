package basic;


import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by BFD_278 on 2015/10/16.
 */
public class ArrayListTest {

    public static void main(String[] args){
        List<String> key_list = new ArrayList<String>();

        String[] str_array = {"Hello", "fuyu", "world"};
        for(int i = 0; i < str_array.length; ++i){
            //key_list.set(i, str_array[i]);
            key_list.add(str_array[i]);
        }
        String key = StringUtils.join(key_list, "/");
        System.out.println("Before set action, The key String is " + key);

        String[] str_array1 = {"Hello1", "fuyu1", "world1", "word2"};
        for(int i = 0; i < str_array1.length; ++i){
            if(key_list.size() < i + 1){
                key_list.add(str_array1[i]);
            }
            else{
                key_list.set(i, str_array1[i]);
            }
        }
        key = StringUtils.join(key_list, "/");
        System.out.println("After set action, The key String is " + key);

    }
}
