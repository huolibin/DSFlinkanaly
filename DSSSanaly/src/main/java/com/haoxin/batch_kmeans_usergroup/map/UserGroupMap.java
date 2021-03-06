package com.haoxin.batch_kmeans_usergroup.map;

import com.haoxin.batch_kmeans_usergroup.util.UserGroupInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/2 10:51
 *
 * 对数据进行切分，组合成UserGroupInfo数据。
 */
public class UserGroupMap  implements MapFunction<String, UserGroupInfo> {
    @Override
    public UserGroupInfo map(String s) throws Exception {
        if (StringUtils.isBlank(s)){
            return null;
        }

        String[] orderinfos = s.split(",");
        String id= orderinfos[0];
        String productid = orderinfos[1];
        String producttypeid = orderinfos[2];
        String createtime = orderinfos[3];
        String amount = orderinfos[4];
        String paytype = orderinfos[5];
        String paytime = orderinfos[6];
        String paystatus = orderinfos[7];
        String couponamount = orderinfos[8];
        String totalamount = orderinfos[9];
        String refundamount = orderinfos[10];
        String num = orderinfos[11];
        String userid = orderinfos[12];

        UserGroupInfo userGroupInfo = new UserGroupInfo();
        userGroupInfo.setUserid(userid);
        userGroupInfo.setCreatetime(createtime);
        userGroupInfo.setAmount(amount);
        userGroupInfo.setPaytype(paytype);
        userGroupInfo.setPaytime(paytime);
        userGroupInfo.setPaystatus(paystatus);
        userGroupInfo.setCouponamount(couponamount);
        userGroupInfo.setTotalamount(totalamount);
        userGroupInfo.setRefundamount(refundamount);
        userGroupInfo.setCount(Long.valueOf(num));
        userGroupInfo.setProducttypeid(producttypeid);
        userGroupInfo.setGroupfield(userid);
        ArrayList<UserGroupInfo> list = new ArrayList<>();
        list.add(userGroupInfo);
        userGroupInfo.setList(list);
        return userGroupInfo;
    }
}
