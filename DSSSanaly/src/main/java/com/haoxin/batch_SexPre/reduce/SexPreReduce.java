package com.haoxin.batch_SexPre.reduce;

import com.haoxin.batch_SexPre.util.SexPreInfo;
import com.haoxin.batch_baijia.util.BaiJiaInfo;
import com.haoxin.stream_matrix.util.CreateDataSet;
import com.haoxin.stream_matrix.util.Logistic;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/14 17:10
 * <p>
 * 对sex的回归预测
 */
public class SexPreReduce implements GroupReduceFunction<SexPreInfo,ArrayList<Double>> {
    @Override
    public void reduce(Iterable<SexPreInfo> iterable, Collector<ArrayList<Double>> collector) throws Exception {
        Iterator<SexPreInfo> iterator = iterable.iterator();
        CreateDataSet trainingSet = new CreateDataSet();

        while (iterator.hasNext()){
            SexPreInfo sexPreInfo = iterator.next();
            int userid = sexPreInfo.getUserid();
            long ordernum = sexPreInfo.getOrdernum();//订单的总数
            long orderfre = sexPreInfo.getOrderfre();//隔多少天下单
            int manclothes = sexPreInfo.getManclothes();//浏览男装次数
            int womenclothes = sexPreInfo.getWomenclothes();//浏览女装的次数
            int childclothes = sexPreInfo.getChildclothes();//浏览小孩衣服的次数
            int oldmanclothes = sexPreInfo.getOldmanclothes();//浏览老人的衣服的次数
            double avramount = sexPreInfo.getAvramount();//订单平均金额
            int producttimes = sexPreInfo.getProducttimes();//每天浏览商品数
            int label = sexPreInfo.getLabel();//0男，1女

            ArrayList<String> as = new ArrayList<>();
            as.add(ordernum+"");
            as.add(orderfre+"");
            as.add(manclothes+"");
            as.add(womenclothes+"");
            as.add(childclothes+"");
            as.add(oldmanclothes+"");
            as.add(avramount+"");
            as.add(producttimes+"");

            trainingSet.data.add(as);
            trainingSet.labels.add(label+"");
        }

        ArrayList<Double> weights = new ArrayList<>();
        weights = Logistic.gradAscent1(trainingSet,trainingSet.labels,500);
        collector.collect(weights);
    }
}
