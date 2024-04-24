package org.example;

import java.sql.*;
import java.util.*;

public class DBTabField {

    /*
    * 获取源表的主键或者唯一键
    * */

    public static Map<String, List<String>> GetPriKeyOrUniKey(String url,
                                                           String schema,
                                                           String username,
                                                           String password,
                                                           String[] tables) {

        Map<String, List<String>> res = new HashMap<>();

        try {
            Connection connection = DriverManager.getConnection(url, username, password);
            Statement statement = connection.createStatement();

            for (String tableName : tables) {
                String query = "SELECT COLUMN_NAME" +
                        " FROM INFORMATION_SCHEMA.COLUMNS" +
                        " WHERE TABLE_SCHEMA = '" + schema + "'" +
                        " and TABLE_NAME = '" +  tableName + "'" +
                        " AND COLUMN_KEY = 'PRI'";
                //System.out.println(query);
                ResultSet resultSet = statement.executeQuery(query);

                List<String> columnNames = new ArrayList<>();

                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    //System.out.println(columnName);
                    columnNames.add(columnName);
                }
                if(columnNames.size() == 0){
                    query = "SELECT COLUMN_NAME" +
                            " FROM INFORMATION_SCHEMA.COLUMNS" +
                            " WHERE TABLE_SCHEMA = '" + schema + "'" +
                            " and TABLE_NAME = '" +  tableName + "'" +
                            " AND COLUMN_KEY = 'UNI'";
                    resultSet = statement.executeQuery(query);
                    while (resultSet.next()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        columnNames.add(columnName);
                    }
                }
                res.put(tableName, columnNames);
            }

            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        return res;
    }

    public static Map<String, Map<String,String>> GetTableFieldsWithType(String url,
                                                                 String database,
                                                                 String username,
                                                                 String password,
                                                                 String[] tables) {

        Map<String, Map<String,String>> res = new HashMap<>();

        try (Connection connection = DriverManager.getConnection(url, username, password);
             Statement statement = connection.createStatement()) {

            for (String tableName : tables) {

                Map<String,String> colNameWithTypeMap = new HashMap<>();
                String query = "SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE " +
                        "TABLE_SCHEMA = '" + database + "'" +
                        " AND TABLE_NAME = '" + tableName + "'";

                ResultSet resultSet = statement.executeQuery(query);

                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String dataType = resultSet.getString("DATA_TYPE");
                    //System.out.println(columnName + ": " + dataType);
                    colNameWithTypeMap.put(columnName, dataType);
                }
                res.put(tableName, colNameWithTypeMap);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return res;
    }


    public static Map<String, List<String>> GetSinkTableFields(String url,
                                                           String schema,
                                                           String user,
                                                           String password,
                                                           String[] tables) {

        //String url = String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, database);
        Map<String, List<String>> res = new HashMap<>();
        try {
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            for(String tableName : tables) {
                String query = "SELECT COLUMN_NAME FROM " +
                        "ALL_TAB_COLUMNS WHERE " +
                        "OWNER = '" + schema + "' AND " +
                        "TABLE_NAME = '" + tableName + "'";
                ResultSet resultSet = statement.executeQuery(query);
                List<String> columnNames = new ArrayList<>();

                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    columnNames.add(columnName);
                }
                res.put(tableName, columnNames);
            }

            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return res;
    }


    /*
    * 创建upsert语句
    * */


    public static void main(String[] args) throws ClassNotFoundException, SQLException {
//        System.out.println("'1".replace("'", "\""));
//        System.exit(1);
//
//        Class.forName("oracle.jdbc.OracleDriver");
//        Connection connection = DriverManager
//                .getConnection("jdbc:oracle:thin:@10.129.37.85:1521:rodsdb"
//                ,"dp_sync","Demo_1234");
//        connection.setAutoCommit(false);
//        Statement statement = connection.createStatement();
//        statement.execute("UPDATE ADS.\"user\" SET \"NUM\" = 1.1000000000," +
//                "\"ID\" = '1' WHERE \"ID\" = '1'");
//        connection.commit();
//        System.exit(1);



//        String server = "127.0.0.1";
//        String database = "flinkcdc";
//        String user = "root";
//        String password = "xw123456";
//        String[] tables = {"user"};
        String server = "10.129.37.115";
        String database = "xuke";
        String user = "dp_sync";
        String password = "DP#Sync_cifi1234";
        String[] tables = {"b_broker_task"};
    }
}
