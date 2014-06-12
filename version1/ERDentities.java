package version1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*Class - creates a Map of ERD Challege Freebase snapshot
 *  Typical record structure : FreebaseID \t anchor \t wikipedia_link 
 *  */
public class ERDentities {

	Map<String, String> EntityIdMap = new HashMap<String, String>();
	
	public Map<String, String> getEntityIdMap(){
		return this.EntityIdMap;
	}
	
	public void clearEntityIdMap(){
		this.EntityIdMap.clear();
	}
	
	/*Loads EntityIdMap - map of FreebaseID to wikiTitle, from the ERD14 file entity.tsv*/
	public void loadEntityIdMap(){
		try{
			System.out.println("Indexing .. ");
			BufferedReader bfr = new BufferedReader(new FileReader("/path/ERD14/entity.tsv"));

			String line = "";
			while ( (line = bfr.readLine()) != null ) {

				if(line.contains("@en")){ 
					String[] linesplits = line.split("\\t"); //System.out.println("FreebaseId = "+linesplits[0]);
					String wikiTitle = linesplits[2].split("/")[3];
					wikiTitle = wikiTitle.substring(0, wikiTitle.length()-1);
					EntityIdMap.put(linesplits[0], wikiTitle);
				}			

			}
			bfr.close();
		} catch(IOException ioe){
			ioe.printStackTrace();
		}
	}//End loadEntityIdMap()
	
	/*Return FreebaseID of given wikiTitle*/
	public String getFreebaseERDid(String wikiTitle){		
	    for (String entry : EntityIdMap.keySet()) {
	        if (EntityIdMap.get(entry).equals(wikiTitle)) {
	            return entry;
	        }
	    }
	    return null;
	}
	
	/*Return wikiTitle of given FreebaseID*/
	public String getERDtitle(String pageId){
		if (EntityIdMap.containsKey(pageId)){
			return EntityIdMap.get(pageId);
		}	    
	    return null;
	}
	
	/*Return true if the given FreebaseID is present in ERD14 Freebase snapshot*/
	public boolean checkERDid(String  pageId){
		return EntityIdMap.containsKey(pageId);
	}
	
	
	public static void main(String[] args) {
		ERDentities erdEntities = new ERDentities();
		erdEntities.loadEntityIdMap();
		System.out.println(erdEntities.EntityIdMap.size()+" Entries Loaded ");
		System.out.println(erdEntities.getFreebaseERDid("Torrey$002C_Utah"));
		System.out.println(erdEntities.checkERDid("/m/010h7d"));
		System.out.println(erdEntities.getERDtitle("/m/010h7d"));

	}

}
