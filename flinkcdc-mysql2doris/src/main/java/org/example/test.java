package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @创建人 君子固穷
 * @创建时间 2024-03-18
 * @描述
 */
public class test {

    public static void main(String[] args) {
        String url = "jdbc:mysql://10.129.36.6:9030/flinkcdc";
        try {
            Connection connection = DriverManager.getConnection(url, "etl", "etl");
            Statement statement = connection.createStatement();
            statement.execute("truncate table flinkcdc.`user2`");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
