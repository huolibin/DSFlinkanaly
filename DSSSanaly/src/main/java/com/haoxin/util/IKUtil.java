package com.haoxin.util;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/6/5 10:12
 */
public class IKUtil {
    private static IKAnalyzer anal = new IKAnalyzer(true);

    public static List<String> getIkWord(String word) {
        StringReader reader = new StringReader(word);
        //分词
        TokenStream ts = null;
        try {
            ts = anal.tokenStream("", reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
        ArrayList<String> arrayList = new ArrayList<>();
        //遍历分词数据
        try {
            while (ts.incrementToken()) {
                String result = term.toString();
                arrayList.add(result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            reader.close();
        }
        return arrayList;
    }

}
