package cn.bfd.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Created by BFD_278 on 2015/7/23.
 */
public class FileUtils {

	public static void FillValMap(Map<String, Integer> map, String fin, String sep,FileSystem fs){
        try{
            String line = null;
            InputStream in=fs.open(new Path(fin));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            
            while((line = br.readLine()) != null){
                String[] items = line.trim().split(sep);
                if(items.length != 2 ){
                    continue;
                }
                map.put(items[0], Integer.valueOf(items[1]));
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    
    public static void FillMap(Map<String, Integer> map, String fin, String sep,FileSystem fs){
        try{
            String line = null;
            InputStream in = fs.open(new Path(fin));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            while((line = br.readLine()) != null){
                String[] items = line.trim().split(sep);
                if(items.length != 1 ){
                    continue;
                }
                map.put(items[0], 1);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 将文件中的每一行数据存储到List中
     * @param list 存储每一行的结果
     * @param fin 读取的文件名
     * @param fs fin文件的HDFS的路径
     */
    public static void FillList(List<String> list, String fin,FileSystem fs){
        try{
            String line = null;
            InputStream in = fs.open(new Path(fin));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            while((line = br.readLine()) != null){
                String item = line.trim();
                list.add(item);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }



    

    public static void FillStrValMap(Map<String, String> map, String fin, String sep, FileSystem fs){
        try{
        	String line = null;
            InputStream in=fs.open(new Path(fin));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            while((line = br.readLine()) != null){
                String[] items = line.trim().split(sep);
                if(items.length != 2 ){
                    continue;
                }
                map.put(items[0], items[1]);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    
    public static void FillValMap(Map<String, Integer> map, String fin, String sep){
        try{
            String line = null;
            BufferedReader br = new BufferedReader(new FileReader(new File(fin)));
            while((line = br.readLine()) != null){
                String[] items = line.trim().split(sep);
                if(items.length != 2 ){
                    continue;
                }
                map.put(items[0], Integer.valueOf(items[1]));
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void FillMap(Map<String, Integer> map, String fin, String sep){
        try{
            String line = null;
            BufferedReader br = new BufferedReader(new FileReader(new File(fin)));
            while((line = br.readLine()) != null){
                String[] items = line.trim().split(sep);
                if(items.length != 1 ){
                    continue;
                }
                map.put(items[0], 1);
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }
    }


}
