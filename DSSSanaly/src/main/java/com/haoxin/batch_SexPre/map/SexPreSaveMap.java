package com.haoxin.batch_SexPre.map;

import com.haoxin.batch_SexPre.util.SexPreInfo;
import com.haoxin.stream_matrix.util.Logistic;
import com.haoxin.util.HbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.Random;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 16:56
 * <p>
 * map方法对源数据处理
 * 成SexPre的格式
 *
 */
public class SexPreSaveMap implements MapFunction<String, SexPreInfo> {
    private ArrayList<Double> weights = null;
    public SexPreSaveMap(ArrayList<Double> weights){
        this.weights = weights;
    }

    @Override
    public SexPreInfo map(String s) throws Exception {
        if (StringUtils.isBlank(s)) {
            return null;
        }
        String[] temps = s.split("\t");
        Random random = new Random();

        int userid = Integer.valueOf(temps[0]);
        long ordernum = Long.valueOf(temps[1]);//订单的总数
        long orderfre = Long.valueOf(temps[4]);//隔多少天下单
        int manclothes = Integer.valueOf(temps[5]);//浏览男装次数
        int womenclothes = Integer.valueOf(temps[6]);//浏览女装的次数
        int childclothes = Integer.valueOf(temps[7]);//浏览小孩衣服的次数
        int oldmanclothes = Integer.valueOf(temps[8]);//浏览老人的衣服的次数
        double avramount = Double.valueOf(temps[9]);//订单平均金额
        int producttimes = Integer.valueOf(temps[10]);//每天浏览商品数


        ArrayList<String> as = new ArrayList<>();
        as.add(ordernum+"");
        as.add(orderfre+"");
        as.add(manclothes+"");
        as.add(womenclothes+"");
        as.add(childclothes+"");
        as.add(oldmanclothes+"");
        as.add(avramount+"");
        as.add(producttimes+"");

        String sexflag = Logistic.classifyVector(as, weights);
        String sexstring = sexflag == "0"?"女":"男";

        HbaseUtil.putdata("userflaginfo",userid+"","baseinfo","sex",sexstring);

        return null;
    }
}
