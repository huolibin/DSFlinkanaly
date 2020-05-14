package com.haoxin.batch_eamil.map;

import com.alibaba.fastjson.JSONObject;
import com.haoxin.batch_eamil.util.EmailInfo;
import com.haoxin.batch_order.util.OrderInfo;
import com.haoxin.batch_order.util.ProductAnaly;
import com.haoxin.util.DateUtils;
import com.haoxin.util.EmailUtils;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 12:26
 * 是对每个月的数据分类
 */
public class EmailMap implements MapFunction<String, EmailInfo> {
    @Override
    public EmailInfo map(String s) throws Exception {
        if(StringUtils.isBlank(s)){
            return null;
        }
        String[] split = s.split(",");
        String userid = split[0];
        String username = split[1];
        String sex = split[2];
        String telphone = split[3];
        String email = split[4];
        String age = split[5];
        String registerTime = split[6];
        String usertype = split[7]; //终端类型

        String emailtypeBy = EmailUtils.getEmailtypeBy(email);

        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "baseinfo";
        String colum = "emailinfo";//运营商

        HbaseUtil.putdata(tablename,rowkey,famliyname,colum,emailtypeBy);//记录写入hbase

        EmailInfo emailInfo = new EmailInfo();
        emailInfo.setEmailtype(emailtypeBy);
        emailInfo.setCount(1l);
        emailInfo.setGroupfield("emailtype=="+emailtypeBy);
        return  emailInfo;
    }
}
