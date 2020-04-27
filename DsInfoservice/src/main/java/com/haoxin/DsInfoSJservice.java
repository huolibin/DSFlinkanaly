package com.haoxin;

import com.alibaba.fastjson.JSON;
import com.haoxin.log.KafkaMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/3 18:13
 */
@Controller
@RequestMapping("DsInfoSJservice")
public class DsInfoSJservice {
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping(value="webInfoSJService",method = RequestMethod.POST)
    public void webInfoSJService(@RequestBody String jsonstr, HttpServletRequest request, HttpServletResponse response){
        System.out.println("未转换kafkamessage之前的数据== " + jsonstr);
        KafkaMessage kafkaMessage = new KafkaMessage();
        kafkaMessage.setJsonmessage(jsonstr);
        kafkaMessage.setCount(1);
        kafkaMessage.setTimestamp(new Date().getTime());
        jsonstr = JSON.toJSONString(kafkaMessage);
        System.out.println("转换之后kafkamessage的数据== " + jsonstr);

        //业务开始
        kafkaTemplate.send("testmessage","key",jsonstr);

//        kafkaTemplate.send("testmessage","key",jsonstr);
        //业务结束
        PrintWriter writer = getWriter(response);
        response.setStatus(HttpStatus.OK.value());
        writer.write("success");
        closeprintwriter(writer);
    }

    //自定义一个reponse的方法
    private PrintWriter getWriter(HttpServletResponse response){
        response.setCharacterEncoding("utf-8");
        response.setContentType("application/json");

        ServletOutputStream out =null;
        PrintWriter printWriter = null;
        try {
            out = response.getOutputStream();
            printWriter = new PrintWriter(out);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return printWriter;
    }

    //自定义一个close方法
    private void closeprintwriter(PrintWriter printWriter){
        printWriter.flush();
        printWriter.close();
    }
}
