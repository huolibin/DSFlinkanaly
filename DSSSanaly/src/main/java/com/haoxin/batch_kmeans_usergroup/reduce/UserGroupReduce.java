package com.haoxin.batch_kmeans_usergroup.reduce;

import com.haoxin.batch_kmeans_usergroup.util.UserGroupInfo;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/2 11:02
 * 对UserGroupInfo的数据reudce，list中有所有信息，所以累加list就可以
 */
public class UserGroupReduce implements ReduceFunction<UserGroupInfo> {
    @Override
    public UserGroupInfo reduce(UserGroupInfo userGroupInfo, UserGroupInfo t1) throws Exception {

        String userid = userGroupInfo.getUserid();
        List<UserGroupInfo> list1 = userGroupInfo.getList();
        List<UserGroupInfo> list2 = t1.getList();

        UserGroupInfo userGroupInfo1 = new UserGroupInfo();
        ArrayList<UserGroupInfo> list = new ArrayList<>();
        list.addAll(list1);
        list.addAll(list2);

        userGroupInfo1.setUserid(userid);
        userGroupInfo1.setList(list);

        return userGroupInfo1;
    }
}
