package cn.bfd.tools;

import java.io.*;
import java.util.List;

/**
 * Created by yu.fu on 2015/11/11.
 */
public class CommonUtils {

    public static final String reverseString(String str) {
        char c[] = str.toCharArray();
        char t;
        for (int i = 0; i < (str.length() + 1) / 2; i++) {
            t = c[i];
            c[i] = c[c.length - i - 1];
            c[c.length - i - 1] = t;
        }
        String str2 = new String(c);
        return str2;
    }


    public static boolean FillArrayList(List<String> list, String fin, String sep, int num, int index){
        if(index >= num){
            System.out.println("The index " + num + " is more than num " + num +".");
            return false;
        }
        try{
            String line = null;
            BufferedReader br = new BufferedReader(new FileReader(new File(fin)));
            while((line = br.readLine()) != null){
                String[] items = line.trim().split(sep);
                if(items.length != num ){
                    continue;
                }
               list.add(items[index]);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
            return false;
        }catch(IOException e){
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
