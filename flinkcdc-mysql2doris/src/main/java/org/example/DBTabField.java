package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBTabField {
    public static Map<String, List<String>> GetTableFields(String host,
                                                 Integer port,
                                                 String database,
                                                 String user,
                                                 String password,
                                                 String[] tables) {

        String url = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        Map<String, List<String>> res = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(url, user, password);
             Statement statement = connection.createStatement()) {
            for(String tableName : tables) {
                String query = "DESCRIBE " + tableName;
                ResultSet resultSet = statement.executeQuery(query);
                List<String> columnNames = new ArrayList<>();

                while (resultSet.next()) {
                    String columnName = resultSet.getString("Field");
                    //System.out.println(columnName);
                    columnNames.add(columnName);
                }
                res.put(tableName, columnNames);
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return res;
    }

    public static void main(String[] args) {

    }
}
