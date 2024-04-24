package org.example.common;

import java.util.*;

public class CommonConfig {

    public static String JobName = "flinkcdc";
    public static String SourceHost = "127.0.0.1";
    public static Integer SourcePort = 3306;
    public static String SourceUser = "";
    public static String SourcePassword = "";
    public static String SourceDatabase = "flinkcdc";
    public static String SourceSchema = "";
    public static String SinkSchema = "";
    public static String SourceTable = "user";


    public static String SinkSID = "";
    public static String SinkHost = "";
    public static Integer SinkPort = 8030;
    public static String SinkUser = "";
    public static String SinkPassword = "";
    public static String SinkDatabase = "flinkcdc";
    public static String TablePrefix = "";
    public static String SinkLabel = "";

    //每一批数据的批次大小或者间隔时间，任意条件满足，触发窗口处理
    public static Long intervals = 5000L;   //单位ms
    public static Integer batchSize = 0x1FC0;  //批次大小

    public static String operation = "-1";  //用于标记任务是start还是restart，start需要清空目标表

    public static List<String> Tab2Tab = new ArrayList<>(); //user,id,name-user1,id,name1
    //多表映射即字段映射
    public static Map<String, String> diyTab2Tab = new HashMap<>(); //table:table1

    public static Map<String, List<String>> tableFieldsMap = new HashMap<>(); //user:column:column
    public static Map<String, Map<String, String>> diyColMap = new HashMap<>(); //column:column

    public static void ParseTabMapping(List<String> list) {
        for (String s : list) {
            String[] ss = s.split("-");       //[user,id  user1,id1]
            String[] ss1 = ss[0].split(",");  //[user,id]
            String[] ss2 = ss[1].split(",");  //[user1,id1]
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < ss1.length; i++) {
                if (i == 0) {
                    diyTab2Tab.put(ss1[i], ss2[i]);
                } else {
                    map.put(ss1[i], ss2[i]);
                }
            }
            diyColMap.put(ss1[0], map);
        }
    }

    public static Map<String,String> GetColumns(String[] tables){
        Map<String,String> map = new HashMap<>();
        for (String s : tables) {
            List<String> fields = tableFieldsMap.get(s);
            String columns = "";
            for (String filed : fields) {
                if(diyColMap.containsKey(s) && diyColMap.get(s).containsKey(filed)){
                    columns += diyColMap.get(s).get(filed) + ",";
                }else{
                    columns += filed + ",";
                }
            }
            columns = columns.substring(0,columns.length()-1);
            map.put(s,columns);
        }
        return map;
    }


    public static void main(String[] args) {
        PriorityQueue<Integer> pq = new PriorityQueue<>();
        for (int i = 0; i < 10; i++) {
            pq.offer(i);
        }
        pq.poll();
        System.out.println(pq);
    }


}
