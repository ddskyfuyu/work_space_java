package cn.bfd.Reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Created by BFD_278 on 2015/10/16.
 */
public class ProtoBufferDemo {
    public static void main(String[] args){
        Class<?> demo = null;
        //UserProfile2.AttributeInfo
        try{
            demo = Class.forName("cn.bfd.protobuf.UserProfile2.AttributeInfo");
        }catch(Exception e){
            e.printStackTrace();
            return;
        }

        Field[] fields = demo.getDeclaredFields();
        for(int i = 0; i < fields.length; ++i){
            int mo = fields[i].getModifiers();
            String priv = Modifier.toString(mo);

            //the type of attribute
            Class<?> type = fields[i].getType();
            System.out.println(priv + " " + type.getName() + " " + fields[i].getName() + ";");
        }

    }
}
