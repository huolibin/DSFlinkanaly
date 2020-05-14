package com.haoxin.batch_eamil.reduce;

import com.haoxin.batch_eamil.util.EmailInfo;
import com.haoxin.batch_order.util.ProductAnaly;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/12 14:37
 */
public class EmailReduce implements ReduceFunction<EmailInfo> {
    @Override
    public EmailInfo reduce(EmailInfo t1, EmailInfo t2) throws Exception {
        String emailtype = t1.getEmailtype();
        long count1 = t1.getCount();

        long count2 = t2.getCount();

        EmailInfo emailInfo = new EmailInfo();
        emailInfo.setEmailtype(emailtype);
        emailInfo.setCount(count1+count2);

        return emailInfo;
    }
}
