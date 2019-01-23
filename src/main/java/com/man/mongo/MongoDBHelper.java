//package com.man.mongo;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.bson.Document;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import com.mongodb.MongoClient;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoCursor;
//import com.mongodb.client.MongoDatabase;
//import com.mongodb.client.model.CreateCollectionOptions;
//import com.mongodb.client.result.DeleteResult;
//
//public class MongoDBHelper {
//		static Logger logger = LoggerFactory.getLogger(MongoDBHelper.class);
//
//	   public static String ip = "127.0.0.1";
//	    //10.80.21.41 10.80.18.1
//	   public static MongoClient mongoClient = null;// new MongoClient(ip, 27017);
//	    //MongoDatabase实例是不可变的
//	   public static MongoDatabase database;
//	   
//	   public String EMOT_COLLECTION = "qemot_info";
//	   
//	   public String PHOTO_COLLECTION = "qphoto_info";
//	   
//	   public String QMSG_COLLECTION = "qmsg_info";
//	   
//	   public String IMG_COLLECTION = "qphoto_img";
//	   
//	   public String QUSER_COLLECTION = "quser_info";
//	   
//	    //firstDB
//	    public static void connect(String databaseName){
//	        database = mongoClient.getDatabase(databaseName);
//	    }
//	    
//	    public static MongoCollection<Document> getCollection(String collectionName){
//	        //MongoCollection实例是不可变的
//	        if(!collectionExists(collectionName)){
//	            return null;
//	        }
//	        MongoCollection<Document> collection = database.getCollection(collectionName);
//	        return collection;
//	    }
//	    
//	    public static boolean collectionExists(final String collectionName) {
//	         boolean collectionExists = database.listCollectionNames()
//	                .into(new ArrayList<String>()).contains(collectionName);
//	        return collectionExists;
//	    }
//	    
//	    public static void getAllDocuments(MongoCollection<Document> collection){
//	        MongoCursor<Document> cursor = collection.find().iterator();
//	        try {
//	            while (cursor.hasNext()) {
//	                System.out.println(cursor.next().toJson());
//	            }
//	        } finally {
//	            cursor.close();
//	        }
//	    }
//	    
//	    public static String getDocumentFirst(MongoCollection<Document> collection){
//	        Document myDoc = collection.find().first();
//	        return myDoc.toJson();
//	    }
//	    
//	  
//	    /**
//	     * 插入一个Document
//	     * @param collection
//	     * @param doc
//	     */
//	    public static void insertDocument(MongoCollection<Document> collection,Document doc){
//	        collection.insertOne(doc);
//	    }
//	    /**
//	     * 插入多个Document
//	     * @param collection
//	     * @param documents
//	     */
//	    public static void insertManyDocument(MongoCollection<Document> collection,List<Document> documents){
//	        collection.insertMany(documents);
//	    }
//	    /**
//	     * 显示创建集合
//	     */
//	    public static void explicitlyCreateCollection(String collectionName){
//	        database.createCollection(collectionName,
//	                new CreateCollectionOptions().capped(false));
//	    }
//
//	    // 删除集合的所有文档
//	    public static long deleteAllDocument(MongoCollection<Document> collection) {
//	        DeleteResult deleteResult = collection.deleteMany(new Document());
//	        long count = deleteResult.getDeletedCount();
//	        return count;
//	    }
//
//	    // 删除集合
//	    public static void deleteCollection(MongoCollection<Document> collection) {
//	        collection.drop();
//	    }
//
//	    public static void closeDb() {
//	        mongoClient.close();
//	    }
//	    
//	    
//}
