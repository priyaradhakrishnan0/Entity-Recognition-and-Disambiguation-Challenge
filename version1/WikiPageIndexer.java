package version1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import Variables.Variables;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

/*Class - creates a mongoDB of wiki Page Id ti wiki Title
 *  */
public class WikiPageIndexer {
	private static MongoClient mongoClient;
	private static DB db;
	private static DBCollection table;

	public WikiPageIndexer () {
		try {
			MongoClientOptions mongoOptions = new MongoClientOptions.Builder().connectionsPerHost(100).autoConnectRetry(true).socketTimeout(5000).connectTimeout(5000).maxWaitTime(2000).build();
			mongoClient = new MongoClient(Variables.wikiPageIndexer , mongoOptions);		
			db = mongoClient.getDB("wikiPageDB");
			table = db.getCollection("wikiPageTitle");
		} catch (UnknownHostException uke){
			uke.printStackTrace();
		}
	}//End constructor

	public void destroy(){
		mongoClient.close();
	}

	public void indexDoc (int pageId, String title) {
		System.out.println("title = "+title+" PageId ="+pageId);
		BasicDBObject doc = new BasicDBObject("pageId", pageId).append("title", title);
		table.insert(doc);		
	}

	public void indexSortedFile (String filename) {
		try{
			System.out.println("Indexing "+filename);
			BufferedReader bfr = new BufferedReader(new FileReader(filename));
			List<DBObject> listDBO = new ArrayList<DBObject>();
			String line = "";
			int count = 0;
			while ( (line = bfr.readLine()) != null ) {

				if(count%5000 == 0){
					table.insert(listDBO);
					System.out.print(count+" ");
					listDBO.clear();
				} 

				if(line.contains(" ")){ 
					String idStr = line.subSequence(0, line.indexOf(" ")).toString();
					String titleStr = line.subSequence(line.indexOf(" ")+1, line.length()).toString();
					titleStr = titleStr.replace(" ","_");
					if(idStr.matches("\\d+")){
						DBObject doc = new BasicDBObject("pageId", Integer.parseInt(idStr)).append("title", titleStr);
						listDBO.add(doc);
						++count;
					}
				}			

			}
			table.insert(listDBO);// copy any remaining into DB(that was not %5000
			bfr.close();
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}//end indexSortedFile()

	/*Return wikiTitle of the given wiki Page Id*/
	public String getTitle (long pageId) {
		db.requestStart();
		String title = null;			
		BasicDBObject query = new BasicDBObject(); 
		query.put( "pageId", pageId);
		BasicDBObject fields = new BasicDBObject("title",true).append("_id",false);		
		DBCursor curs = table.find(query, fields);//System.out.println("num of results = "+curs.count());
		while(curs.hasNext()) {
			DBObject o = curs.next(); //System.out.println("Link Freq = "+o.get("anchPageFreq").toString());
			title = o.get("title").toString();
		}		
		db.requestDone();
		return title;		
	}//End getTitle()

	public static void main(String[] args) {
		//WikiPageIndexer wikiPageIndexer = new WikiPageIndexer();
		//wikiPageIndexer.indexSortedFile("/home/priya/Desktop/KnowledgeBase/ERD14/idsort.txt");
		//		System.out.println("PageId ="+wikiPageIndexer.getPageId("Actrius"));
		//		System.out.println("Title ="+wikiPageIndexer.getTitle(330));

	}

}
