package dcommon;

import org.bson.Document;
import org.junit.Test;

import com.man.mongo.MongoDBHelper;
import com.mongodb.client.MongoCollection;

public class TestMango {

	@Test
	public void test01() {
		String databaseName = "bdata_16";
	    String collectionName = "quser_info";
	    MongoCollection<Document> firstCollection;
	    MongoDBHelper.connect(databaseName);
	    firstCollection =  MongoDBHelper.getCollection(collectionName);
	}
	
	@Test
	public void test02() {
		String databaseName = "bdata_16";
	    String collectionName = "quser_info";
	    MongoCollection<Document> firstCollection;
	    MongoDBHelper.connect(databaseName);
	    firstCollection =  MongoDBHelper.getCollection(collectionName);
	    String json = MongoDBHelper.getDocumentFirst(firstCollection);
        System.out.println(json);
	}
	
}
