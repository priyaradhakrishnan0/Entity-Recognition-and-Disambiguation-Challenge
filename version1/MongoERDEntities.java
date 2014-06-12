package version1;

import java.net.UnknownHostException;

import Variables.Variables;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

/*Class - creates a mongoDB RD Challege Freebase snapshot
 *  Typical record structure : FreebaseID \t anchor \t wikipedia_link 
 *  */

public class MongoERDEntities {
	public static MongoClient mongoClient; 
	public static DB db;
	public static DBCollection table;
	
	/*Constuctor*/
	public MongoERDEntities() {
		try {
			MongoClientOptions mongoOptions = new MongoClientOptions.Builder().connectionsPerHost(100).autoConnectRetry(true).socketTimeout(5000).connectTimeout(5000).maxWaitTime(2000).build();
			mongoClient = new MongoClient(Variables.ERDentities, mongoOptions); 	
			db = mongoClient.getDB("entities");
			table = db.getCollection("freebase");
		} catch (UnknownHostException uke){
			uke.printStackTrace();
		}
	}//End constructor
	
	public void destroy(){
		mongoClient.close();
	}
	
	/*Gets the freebase ID for corrosponding wikipedia Page from MongoDB.
	* If the Page is not available in ERD Entities, null is returned*/
	public String getFreebaseId (String title) {
		db.requestStart();
		BasicDBObject query = new BasicDBObject(); 
		query.put( "wikiTitle", title);
		BasicDBObject fields = new BasicDBObject("freebaseID",true).append("_id",false);		
		DBObject curs = table.findOne(query, fields);
		String freebaseID = null;
		if(curs != null) {
			freebaseID = (String) curs.get("freebaseID");
		}		
		db.requestDone();
		return freebaseID;		
	}//End getFreebaseId()	
	
	public static void main(String[] args) {
		MongoERDEntities mee = new MongoERDEntities();
		System.out.println(mee.getFreebaseId("India"));
	}

}
