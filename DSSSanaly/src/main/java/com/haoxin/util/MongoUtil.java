package com.haoxin.util;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Arrays;


/**
 * @author huolibin@haoxin.cn
 * @date Created by sheting on 2020/5/13 16:18
 */
public class MongoUtil {

    //有用户密码时登录
    static {
        MongoCredential credential = MongoCredential.createCredential("user", "mydb", "passwd".toCharArray());
        ServerAddress serverAddress = new ServerAddress("192.168.71.10", 27017);

        MongoClient mongoClient1 = new MongoClient(serverAddress, Arrays.asList(credential));
    }

    //无密码时登录
    private static MongoClient mongoClient = new MongoClient("192.168.71.10", 27017);



    //find查询
     public static Document findoneby(String tablename,String database, String yearbasetype){

         MongoDatabase mydb = mongoClient.getDatabase(database);
         MongoCollection<Document> mongoCollection = mydb.getCollection(tablename);
         Document doc = new Document();
         doc.put("info",yearbasetype);
         FindIterable<Document> documents = mongoCollection.find(doc);
         MongoCursor<Document> iterator = documents.iterator();
         while (iterator.hasNext()){
             return iterator.next();
         }
         return null;
     }

     //保存或者更新
    public static void saveorupdatemongo(String tablename,String database, Document document){
        MongoDatabase database1 = mongoClient.getDatabase(database);
        MongoCollection<Document> collection = database1.getCollection(tablename);
        if (!document.containsKey("_id")){
            ObjectId objectId = new ObjectId();
            document.put("_id",objectId);
            collection.insertOne(document);
            return;
        }
        Document document1 = new Document();
        String objectid = document.get("_id").toString();
        document1.put("_id",new ObjectId(objectid));
        FindIterable<Document> findIterable = collection.find(document1);
        if (findIterable.iterator().hasNext()){
            collection.updateOne(document1,new Document("$set",document));
            System.out.println("update: "+JSONObject.toJSON(document));
        }else {
            collection.insertOne(document);
            System.out.println("insert: "+JSONObject.toJSON(document));
        }
    }

}
