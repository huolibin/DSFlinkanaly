package com.haoxin.batch_baijia.map;

import com.haoxin.batch_baijia.util.BaiJiaInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 16:56
 * <p>
 * map方法对源数据处理
 * 成BaiJiaInbfo的格式
 */
public class BaiJiaMap implements MapFunction<String, BaiJiaInfo> {
    @Override
    public BaiJiaInfo map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }

        String[] orderinfos = s.split(",");
        String id = orderinfos[0];
        String productid = orderinfos[1];//产品id
        String producttypeid = orderinfos[2];//产品类型
        String createtime = orderinfos[3];
        String amount = orderinfos[4];//产品金额
        String paytype = orderinfos[5];//支付类型
        String paytime = orderinfos[6];
        String paystatus = orderinfos[7];//支付状态(1是支付完成)
        String couponamount = orderinfos[8];//优惠金额
        String totalamount = orderinfos[9];//总金额
        String refundamount = orderinfos[10];//退还金额
        String num = orderinfos[11];
        String userid = orderinfos[12];

        BaiJiaInfo baiJiaInfo = new BaiJiaInfo();
        baiJiaInfo.setUserid(userid);
        baiJiaInfo.setCreatetime(createtime);
        baiJiaInfo.setPaytime(paytime);
        baiJiaInfo.setPaytype(paytype);
        baiJiaInfo.setAmount(amount);
        baiJiaInfo.setPaystatus(paystatus);
        baiJiaInfo.setCouponamount(couponamount);
        baiJiaInfo.setTotalamount(totalamount);
        baiJiaInfo.setRefundamount(refundamount);
        baiJiaInfo.setGroupfield("baijia==" + userid);

        ArrayList<BaiJiaInfo> list = new ArrayList<>();
        list.add(baiJiaInfo);
        baiJiaInfo.setList(list);
        return baiJiaInfo;
    }
}
