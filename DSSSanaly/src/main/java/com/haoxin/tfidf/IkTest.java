package com.haoxin.tfidf;

import com.haoxin.util.IKUtil;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/4 17:28
 * ik分词器的测试
 */
public class IkTest {
    public static void main(String[] args) {
        String test = "我爱你中国，我爱你友凡";
//        //创建分词对象
//        IKAnalyzer anal = new IKAnalyzer(true);
//        StringReader reader = new StringReader(test);
//
//        //分词
//        TokenStream ts = null;
//        try {
//            ts = anal.tokenStream(",", reader);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
//        //遍历分词数据
//        try {
//            while (ts.incrementToken()){
//                String result = term.toString();
//                System.out.println(result);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        reader.close();

        //等同于上面的代码
        List<String> ikWord = IKUtil.getIkWord(test);
        String s = ikWord.toString();
        System.out.println(s);
    }
}
