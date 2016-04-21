package basic;

/**
 * Created by BFD_278 on 2015/10/21.
 */
public class TestSplit {

    public static void main(String args[]){
        String testStr = "北京市$北京";


        String[] strArray = testStr.trim().split("$");

        System.out.println("The length of strArray: " + strArray.length);


    }
}
