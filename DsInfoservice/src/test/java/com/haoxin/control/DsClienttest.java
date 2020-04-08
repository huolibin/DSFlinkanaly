package com.haoxin.control;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/7 15:52
 * 模拟电商客户端
 */
public class DsClienttest {
    public static void main(String[] args) {
        String message = "asdfgh";
        String address = "http://127.0.0.1:6097/DsInfoSJservice/webInfoSJService";
        try {
            URL url = new URL(address);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setAllowUserInteraction(true);
            conn.setUseCaches(false);
            conn.setReadTimeout(6*1000);
            conn.setRequestProperty("User-Agent","Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36");
            conn.setRequestProperty("Content-Type","application/json");
            conn.connect();

            OutputStream outputStream = conn.getOutputStream();
            BufferedOutputStream out = new BufferedOutputStream(outputStream);
            out.write(message.getBytes());
            out.flush();
            String temp = "";
            InputStream in = conn.getInputStream();
            byte[] bytes = new byte[1024];
            while (in.read(bytes,0,1024) != -1){
                temp+= new String(bytes);
            }
            System.out.println(conn.getResponseCode());
            System.out.println(temp);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
