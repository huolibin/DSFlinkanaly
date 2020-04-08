package com.haoxin.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/4/8 11:38
 */

@RestController
@RequestMapping("/kafka")
public class CollectController {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping(value = "/send",method = RequestMethod.GET)
    public void sendKafka(HttpServletRequest request, HttpServletResponse response){
        try {
            String message = request.getParameter("message");
            System.out.println("kafka的消息={}"+message);
            kafkaTemplate.send("test","key",message);
            logger.info("发送kafka成功。");
        } catch (Exception e) {
            logger.error("发送kafka失败",e);
        }
    }
}
