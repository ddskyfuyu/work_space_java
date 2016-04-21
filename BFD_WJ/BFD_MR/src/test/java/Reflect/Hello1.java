package Reflect;

/**
 * Created by BFD_278 on 2015/10/16.
 */
public class Hello1 {

    public static void main(String[] args){
        Class<?> demo1 = null;
        Class<?> demo2 = null;
        Class<?> demo3 = null;

        try{
            demo1 = Class.forName("Reflect.Demo");
        }catch (Exception e){
            e.printStackTrace();
        }

        demo2 = new Demo().getClass();
        demo3 = Demo.class;

        System.out.println("Class Name:"  + demo1.getName());
        System.out.println("Class Name:" + demo2.getName());
        System.out.println("Class Name:" + demo3.getName());
    }
}
