package version1;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.lang.Math;

import Variables.Variables;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
/*Class - creates a mongoDB of anchor to wiki inlink page index
 *  Typical record wiki_title, link[ link1, link2, .. linkN]
 *  */
public class InlinkIndex {
	private static MongoClient mongoClient; 
	private static DB db;
	private static DBCollection table;
	
	//constructor
	public InlinkIndex() {
		try {
			MongoClientOptions mongoOptions = new MongoClientOptions.Builder().connectionsPerHost(100).autoConnectRetry(true).socketTimeout(5000).connectTimeout(5000).maxWaitTime(2000).build();
			mongoClient = new MongoClient(Variables.inlinkIndexer , mongoOptions);		
			db = mongoClient.getDB("wikilinks");
			table = db.getCollection("inlinks");
		} catch (UnknownHostException uke){
		     uke.printStackTrace();
		}
	}//End constructor
	
	public void destroy(){
		mongoClient.close();
	}
	
	/*Returns total number of in-links to the given wiki_title*/
	public int getInlinkFreq (String anchor) {
		db.requestStart();		
		int inlinkFreq = 0;		
		BasicDBObject query = new BasicDBObject(); 
		query.put( "page", anchor);
		BasicDBObject fields = new BasicDBObject("link",true).append("_id",false);		
		DBObject curs = table.findOne(query, fields);
		if(curs != null) {
		    String catObject = curs.get("link").toString();	
			inlinkFreq = catObject.split(",").length;				
		}		
		db.requestDone();
		return inlinkFreq;		
	}//End getTotalLinkFreq()
	
	/*Returns list of in-links to the given wiki_title*/
	public ArrayList<String> getInlinks (String anchor) {
		db.requestStart();
		ArrayList<String> catList = new ArrayList<String>();
		BasicDBObject query = new BasicDBObject(); 
		query.put( "page", anchor);
		BasicDBObject fields = new BasicDBObject("link",true).append("_id",false);		
		DBObject curs = table.findOne(query, fields);//System.out.println("num of results = "+curs.count());
		if(curs != null) {
		    String catObject = curs.get("link").toString();	//System.out.println(catObject);
    		catObject = catObject.replaceAll("\\[", "");
			catObject = catObject.replaceAll("\\]", "");
			for (String cat :  catObject.split(",")){
				cat = cat.replaceAll("\"", "");
				catList.add(cat);
			}			
		}		
		db.requestDone();
		return catList;		
	}//End getInLinks()
	
	public int getCommonInlinkCount(String title1, String title2){
		ArrayList<String> inList1 = getInlinks(title1);
		ArrayList<String> inList2 = getInlinks(title2);
		HashSet<String> inlinks1 = new HashSet<>();
		HashSet<String> inlinks2 = new HashSet<>();
		inlinks1.addAll(inList1);
		inlinks2.addAll(inList2);
		inlinks1.retainAll(inlinks2);
		return inlinks1.size();
	}
	
	/*Semantic relatedness between two page titles by D.Milne method */
	public double relatedness(String title1, String title2){
		double rel=0.0;
		int inA = getInlinkFreq(title1);
		int inB = getInlinkFreq(title2); // System.out.println("inB = "+inB +", inA = "+inA+" combined inlinks = "+Math.max(inA,inB));
		/*If the two pages do not share common inlinks, they are not related*/
		double comLinks = getCommonInlinkCount(title1, title2);
		if((comLinks > 0.0) && (Math.min(inA, inB) > 0 )){
			double numerator = Math.log((double)Math.max(inA,inB)) - Math.log(comLinks); 
			/*ln 12165935 = 16.3141503911 */
			double denomenator = 16.3141503911 - Math.log(Math.min(inA, inB)); 
			rel = 1 - (numerator / denomenator);
		}
		return rel;
	}//End rel(pa , pb )
	
	public static void main(String[] args) {
		InlinkIndex inlinkIndex = new InlinkIndex();
		//System.out.println(inlinkIndex.getInlinkFreq("Botrydiopsis"));		
		//System.out.println("rel(\"Apple\", \"Dragon\") = "+ inlinkIndex.relatedness("Apple","Pineapple"));	
		System.out.println("rel(Ritz-Carlton_Hotel_Company, Lake_Las_Vegas) = "+ inlinkIndex.relatedness("Ritz-Carlton_Hotel_Company","Lake_Las_Vegas"));
		
	}

}
