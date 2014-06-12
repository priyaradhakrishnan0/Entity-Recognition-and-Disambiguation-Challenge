package version1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import Variables.Variables;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

/*Class - creates a mongoDB of anchor to inlink page index
 *  Typical record anchor|link freq|total freq|page id|freq of anchor in page id
 *  */
public class AnchorIndexer {
	private static MongoClient mongoClient; 
	private static DB db;
	private static DBCollection table;
	/*Constructor*/
	public AnchorIndexer () {
		try {
			MongoClientOptions mongoOptions = new MongoClientOptions.Builder().connectionsPerHost(100).autoConnectRetry(true).socketTimeout(5000).connectTimeout(5000).maxWaitTime(2000).build();
			mongoClient = new MongoClient (Variables.AnchorIndexer , mongoOptions); 			
			db = mongoClient.getDB("anchorDB");
			table = db.getCollection("anchors");
		} catch (UnknownHostException uke){
			uke.printStackTrace();
		}
	}
	
	public void destroy(){
		mongoClient.close();
	}

	public DBObject indexDoc (String anchor, int totalFreq, int pageId, int anchPageFreq) {
		BasicDBObject doc = new BasicDBObject("anchor", anchor.replaceAll("\\s+", " ").replaceAll("–", "-")).
				append("totalFreq", totalFreq).
				append("pageId", pageId).
				append("anchPageFreq", anchPageFreq);
		return doc;
	}

	public void indexSortedFile (String filename) {
		try{
			System.out.println("Indexing "+filename);
			BufferedReader bfr = new BufferedReader(new FileReader(filename));
			String line = "";
			ArrayList<DBObject> anchorArr = new ArrayList<DBObject>();
			int count = 0;
			while ( (line = bfr.readLine()) != null ) {
				if(line.contains("|")){ 
					String[] linesplit = line.split("\\|");
					if(linesplit.length==5){
						anchorArr.add(indexDoc(linesplit[0], Integer.parseInt(linesplit[2]), Integer.parseInt(linesplit[3]), Integer.parseInt(linesplit[4])));
						count++;
						if(count%10000==0){
							table.insert(anchorArr);		
							anchorArr.clear(); //System.out.println(count);
						}
					} else if(linesplit.length>5){
						anchorArr.add(indexDoc(linesplit[0], Integer.parseInt(linesplit[2]), Integer.parseInt(linesplit[3]), Integer.parseInt(linesplit[4])));
						for(int i=5;i<linesplit.length;i=i+2){
							if(linesplit[i].matches("\\d+") && linesplit[i+1].matches("\\d+")){ 	//Numeric string validation test
								anchorArr.add(indexDoc(linesplit[0], 0, Integer.parseInt(linesplit[i]), Integer.parseInt(linesplit[i+1])));
								count++;
								if(count%10000==0){
									table.insert(anchorArr);		
									anchorArr.clear(); //System.out.println(count);
								}
							}
						}//End for
					}
				}			
			}
			if(count%10000!=0){
				table.insert(anchorArr);		
				anchorArr.clear();
				System.out.println(count);
			}
			bfr.close();
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}//end indexSortedFile()

	/* Returns number of times 'anchor' occurs in Wikipedia as a hyperlink */
	public int getTotalLinkFreq (String anchor) {
		db.requestStart();
		int totalFreq = 0;			
		BasicDBObject query = new BasicDBObject(); 
		query.put( "anchor", anchor);
		BasicDBObject fields = new BasicDBObject("anchor_freq",true).append("_id",false);		
		DBObject obj = table.findOne(query, fields);//System.out.println("num of results = "+curs.count());
		if(obj != null) {
			//System.out.println("Link Freq = "+o.get("anchPageFreq").toString());
			totalFreq =  (int)obj.get("anchor_freq");
		}		
		db.requestDone();
		return totalFreq;		
	}//End getTotalLinkFreq()	

	/* Returns number of times 'anchor' occurs in Wikipedia, but is NOT a hyperlink */
	public int getTotalFreq (String anchor) {
		db.requestStart();
		int totalFreq = 0;			
		BasicDBObject query = new BasicDBObject(); // create an empty query 
		query.put( "anchor", anchor);
		BasicDBObject fields = new BasicDBObject("total_freq",true).append("_id",false);		
		DBObject obj = table.findOne(query, fields);//System.out.println("num of results = "+curs.count());
		if(obj != null) { //System.out.println("Freq = "+o.get("totalFreq").toString());
			totalFreq =  (int)obj.get("total_freq");
		}		
		db.requestDone();
		return totalFreq;
	}//End getTotalFreq()
		
	/* Returns List of Wikipedia page-ids of pages the string 'anchor' points to in Wikipedia */
	public List<Long> getPages (String anchor) {
		db.requestStart();
		List<Long> PageCollection = new ArrayList<Long>();			
		BasicDBObject query = new BasicDBObject(); 
		query.put( "anchor", anchor);
		BasicDBObject fields = new BasicDBObject("pages",true).append("_id",false);		
		DBObject obj = table.findOne(query, fields);//System.out.println("num of results = "+curs.count());
		if(obj != null) {
			JSONParser jp = new JSONParser();
			JSONArray jarr = null;
			try {
				jarr = (JSONArray) jp.parse(obj.get("pages").toString());
			} catch (ParseException e) {
				jarr = new JSONArray();
			}
			//System.out.println("Link Freq = "+o.get("anchPageFreq").toString());
			for(int i = 0; i < jarr.size(); i++)
			{
				JSONObject objects = (JSONObject) jarr.get(i);
				PageCollection.add((long)(objects.get("page_id")));
			}
		}		
		db.requestDone();
		return PageCollection;		
	}//End getPages()

	/* Returns map of Wikipedia page-ids to number of inlinks to those pages. Page ids are pages the string 'anchor' points to in Wikipedia */
	public Map<Long, Integer> getPagesMap (String anchor) {
		db.requestStart();
		Map<Long, Integer> PageCollection = new HashMap<Long, Integer>();			
		BasicDBObject query = new BasicDBObject(); 
		query.put( "anchor", anchor);		
		BasicDBObject fields = new BasicDBObject("page_id",true).append("pages", true).append("page_freq", true).append("anchor_freq", true).append("_id",false);		
		DBObject ans = table.findOne(query, fields);//System.out.println("num of results = "+curs.count());
		db.requestDone();
		if(ans != null) {
			JSONParser jp = new JSONParser();
			JSONArray jo = null;
			try {	//System.out.println(ans.get("pages"));
				jo = (JSONArray) jp.parse(ans.get("pages").toString());
			} catch (ParseException e) {
				e.printStackTrace();
			}//System.out.println("Link Freq = "+o.get("anchPageFreq").toString());
			for(int i = 0; i < jo.size(); i++)
			{
			    JSONObject object = (JSONObject) jo.get(i);
				Long pId = (long)(object.get("page_id"));
				Long pValue0 = (long)object.get("page_freq");
				int pValue = pValue0.intValue();
				if(PageCollection.containsKey(pId)){
					pValue = PageCollection.get(pId)+ pValue;
				} 
				PageCollection.put(pId, pValue);
			}
		}
		return PageCollection;		
	}//End getPagesMap()
	
	/* Returns map of Wikipedia page-ids to number of inlinks to those pages. 
	 * Only those pages whose inlinks contribute to 'restriction' % of the total inlinks are returned.
	 * Page ids are pages the string 'anchor' points to in Wikipedia. */
	public Map<Long, Integer> getPagesMap (String anchor, double restriction) {
		db.requestStart();
		Map<Long, Integer> PageCollection = new HashMap<Long, Integer>();			
		BasicDBObject query = new BasicDBObject(); 
		query.put( "anchor", anchor);		
		BasicDBObject fields = new BasicDBObject("page_id",true).append("pages", true).append("page_freq", true).append("anchor_freq", true).append("_id",false);		
		DBObject ans = table.findOne(query, fields);
		db.requestDone();
		if(ans != null) {
			JSONParser jp = new JSONParser();
			JSONArray jo = null;
			int freq = (int)ans.get("anchor_freq");
			try {
				jo = (JSONArray) jp.parse(ans.get("pages").toString());
			} catch (ParseException e) {				
				e.printStackTrace();
			}
			for(int i = 0; i < jo.size(); i++)
			{
			    JSONObject object = (JSONObject) jo.get(i);
				Long pId = (long)(object.get("page_id"));
				Long pValue0 = (long)object.get("page_freq");
				int pValue = pValue0.intValue();
				if(PageCollection.containsKey(pId)){
					pValue = PageCollection.get(pId)+ pValue;
				}
				if(pValue/(1.0*freq)>restriction){
					PageCollection.put(pId, pValue);
				}
			}
		}

		return PageCollection;		
	}//End getPagesMap()
	
	
	/* Returns two member integer array. 
	 * member 0 = total number of inlinks for the string anchor.
	 * member 1 = number of inlinks to given PageId from the String anchor.*/
	public int[] getPageCountInPages (String anchor, long PageId) {
		db.requestStart();
		int[] PageCountResults  = new int[2];;
		int pageCount = 0;
		int totalCount = 0;
		BasicDBObject query = new BasicDBObject(); 
		query.put("anchor", anchor);
		BasicDBObject fields = new BasicDBObject("pages",true).append("anchor_freq",true).append("total_freq",true).append("_id",false);		
		DBObject obj = table.findOne(query, fields);//System.out.println("Pages Total = "+curs.count());
		db.requestDone();
		if(obj!=null) {
			//System.out.println("Obj = "+o.get("pageId").toString());	
			JSONParser jp = new JSONParser();
			JSONArray jarr = null;
			try {
				jarr = (JSONArray) jp.parse(obj.get("pages").toString());
			} catch (ParseException e) {
				jarr = new JSONArray();
			}
			//System.out.println("Link Freq = "+o.get("anchPageFreq").toString());
			for(int i = 0; i < jarr.size(); i++)
			{
				JSONObject jo = (JSONObject) jarr.get(i);
				if(PageId == (long)jo.get("page_id")){
					 Long pageCount0 = (long)jo.get("page_freq"); //++pageCount;
					 pageCount += pageCount0.intValue();
				} 
			}
			totalCount += (int)obj.get("anchor_freq");
		}		
		PageCountResults[1] = pageCount; //System.out.println("Pages matching = "+pageCount);
		PageCountResults[0] = totalCount;
		return PageCountResults;		
	}//End getPageCountInPages()

	/*link-probability - Probability that an occurrence of a is an anchor pointing to some Wikipedia page*/
	public double lp(String anchor){
		int totalFreq = getTotalFreq(anchor);
		int totalLinkFreq = getTotalLinkFreq(anchor);
		if(totalFreq+totalLinkFreq > 0){
			//System.out.println("totFr = "+totalFreq+" totLinkFr = "+totalLinkFreq);
			return totalLinkFreq*1.0/(totalFreq+totalLinkFreq);
		} else {
			return 0;
		}
	}

	/*Get entity mentions of a query string*/
	public ArrayList<String> mentions(String text){
		text = text.toLowerCase();
		text = text.replaceAll("[,.;]"," ").replaceAll("\\s+", " ");
		ArrayList<String> mentions = new ArrayList<String>();
		String split[] = text.split("[ _:.,]");
		int length = split.length;
		for(int i=0;i<length;i++){
			double currLP = 0;
			for(int j=6;j>0;j--){
				if(i+j>length)
					continue;
				String cMention = "";
				for(int k=i;k<i+j&&i+j<=length;k++){
					cMention += split[k].toLowerCase().trim() + " ";
				}
				cMention = cMention.trim();
				//System.out.println(cMention+ " " + lp(cMention));
				if(lp(cMention)>0.01 && !cMention.equals("") && lp(cMention)>currLP){
					mentions.add(cMention.trim());
					currLP = lp(cMention);
					//System.out.println(i+" "+j);
					i = i + j - 1 ;
					break;
				}
			}
		}
		
		String curr = "";
		if(mentions.size()>0)
			curr = mentions.get(0);
		for(int i = 1;i < mentions.size();i++){
			if(curr.contains(mentions.get(i))){
				System.err.println(curr+","+lp(curr) + " " +mentions.get(i)+","+ lp(mentions.get(i)));
				if(lp(curr)*.5>=lp(mentions.get(i))){
					//pruneMentions.add(i);
					mentions.remove(i);
					i = i-1;
				}
				else{
					mentions.remove(curr);
				}
			}
			else{
				curr = mentions.get(i);
			}
		}
		return mentions;
	}

	public static void main(String[] args) {
		//AnchorIndexer anchorIndexer = new AnchorIndexer();

		/*Store index file in mongodb*/
		//		String sortedFile = "/path/index/xet_synonym";
		//		anchorIndexer.indexSortedFile(sortedFile);

		/*absolute freq i.e number of times anchor a occurs in Wikipedia not as an anchor*/
		//		String anchor = "samsung galaxy";
		//		System.out.println("Anchor : "+anchor+", Total freq : "+anchorIndexer.getTotalFreq(anchor));

		/*link(a) = Link freq i.e number of times a occurs in Wikipedia as an anchor*/
		//		String anchor = args[1];
		//		System.out.println("Anchor : "+anchor+", Total anchor freq : "+anchorIndexer.getTotalLinkFreq(anchor));

		/*freq(a) = freq i.enumber of times a occurs in Wikipedia (as an anchor or not)*/
		//		System.out.println("Anchor : "+anchor+", Freq : "+(anchorIndexer.getTotalFreq(anchor)+anchorIndexer.getTotalLinkFreq(anchor)));

		/*Pg(a) = Pages pointed to by anchor a*/
		//		System.out.println("Anchor : "+anchor+", Pages returned : "+(anchorIndexer.getPages(anchor).size()));
		//		for(int pgId:anchorIndexer.getPages(anchor)){
		//			System.out.println("Page : "+pgId);
		//		}

		/*Unique Pages pointed to by anchor a*/
		//		String anchor = "conference";
		//		System.out.println("Anchor : "+anchor+", Pages returned : "+(anchorIndexer.getPages(anchor).size()));
		//
		//		Map<Integer, Integer> Pb = anchorIndexer.getPagesMap(anchor);
		//		System.out.println("Anchor : "+anchor+", Unique Pages returned : "+(Pb.size()));
		//		for(int pgId:Pb.keySet()){
		//			System.out.println("Page : "+pgId+" num : "+Pb.get(pgId));
		//		}

		/*Prior Pr(p/a) = (pages in Pg(a) that is p) / ( Pg(a) )*/
		//		int pgId = Integer.parseInt(args[2]);		
		//		int[] ans = anchorIndexer.getPageCountInPages(anchor, 33364019);
		//		System.out.println("Prior Pr(p/a) = "+((1.0*ans[1]) / ans[0]));
	}

}
