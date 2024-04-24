package org.example.sink;

import org.codehaus.jackson.map.ObjectMapper;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class AlertUtil implements  java.io.Serializable{
    public String DEV_BASE_URL = "http://127.0.0.1:8081"; // Replace with your actual API base URL

    public String PROD_BASE_URL = "http://10.129.80.19:8080";


    public  void sendAlert(String message)  {
        try {
            String urlString = this.PROD_BASE_URL + "/job/alert";
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // 设置请求方法为POST
            connection.setRequestMethod("POST");

            // 允许输入输出流
            connection.setDoOutput(true);
            connection.setDoInput(true);

            // 设置请求头
            connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

            // 构建请求体
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> requestBodyMap = new HashMap<>();
            requestBodyMap.put("message", message);

            String requestBody = objectMapper.writeValueAsString(requestBodyMap);

            //获取输出流并写入请求体
            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(requestBody.getBytes("UTF-8"));
            }

            // 获取响应码
            int responseCode = connection.getResponseCode();
            System.out.println("Response Code: " + responseCode);

            // 关闭连接
            connection.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
