package version1;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import Variables.Variables;
/*Key class of this package.
 * Starting from query string input till entity mention output is handled here. */
public class Disambiguator {

	private static Disambiguator instance = null;
	private static boolean debug;
	private static AnchorIndexer anchorIndexer;
	private static WikiPageIndexer wikiPageIndexer;
	private static RitterChunks rc;
	private static PruneBottomPages pp;
	private static InlinkIndex inlinkIndex;
	private static Pruning pruning;
	private static MongoERDEntities mee;
	public static BufferedWriter bw;
	
	/*Constructor*/
	public Disambiguator(){
		debug = false;
		anchorIndexer = new AnchorIndexer();
		wikiPageIndexer = new WikiPageIndexer();
		rc = new RitterChunks();
		pp = new PruneBottomPages();
		inlinkIndex = new InlinkIndex();
		pruning = new Pruning();
		mee = new MongoERDEntities();
		try {
			bw = new BufferedWriter(new FileWriter(new File(Variables.logs),true));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*Singleton implementation*/
	public static Disambiguator getInstance(){
		if(instance!=null){
			return instance;
		}
		instance = new Disambiguator();
		return instance;
	}
	
	public static void main(String[] args) {
		Disambiguator disambiguator = new Disambiguator();
		
		//disambiguator.getResultsFormatted("0","apple is a fruit");
		//disambiguator.getResultsFormatted("1", "john harmon football player");
		//disambiguator.printFeatures("pakistan news new york");
		disambiguator.printFeatures("apple is a fruit");
		//disambiguator.printFeatures("brooks brothers");
		//disambiguator.printFeatures("bowflex power pro");
		//disambiguator.printFeatures("ritz carlton lake las vegas");
		//disambiguator.printFeatures("I am going to buy apple fruit today");
		//disambiguator.printFeatures("bowflex power pro");
		//disambiguator.getResultsFormatted("1","bowflex power pro");
		
		//disambiguator.printFeatures(args[0]);
		//disambiguator.getResultsFormatted("1", args[0]);
	}

	/*Returns a Map of wikiPage title to entity mention( List of link probability of wikiPage and entity mention)*/
	public HashMap<String, String[]> features(String query){
		HashMap<String,Double> memo = new HashMap<>();
		HashMap<String, String[]> disambiguations = new HashMap<>();
		ArrayList<String> mentionList = anchorIndexer.mentions(query); //System.out.println("Mention count ="+mentionList.size() + " " + mentionList);
		mentionList = rc.mentionPruning(mentionList, query.toLowerCase()); 	//mentionList = rc.stopwordPruning(mentionList, query);
		for(String a:mentionList){ System.out.println("Mention a ="+a);
			memo.clear();
			Map<Long,Integer> Pga = anchorIndexer.getPagesMap(a);//System.out.println("Pa count ="+Pga.size());
			double[] relA = new double[Pga.size()];			
			int PgAiteration = 0;
			String disambiguatedPg = null; Double disambiguatedPgPRA = 0.0; String disambiguatedPgText = null;
			double maxLinearCombination = 0.0, linearCombination = 0.0; ;
			String[] score1 = new String[2];
			score1[0] = "1.0";
			score1[1] = a;
			Long topPage = pp.getTop(a, Pga);
			String fallbackPage = wikiPageIndexer.getTitle(topPage);
			if(debug){
				disambiguations.put(fallbackPage, score1);
				continue;
			}
			int count = 0;
			for(long i:Pga.keySet()){
				count += Pga.get(i);
			}
			disambiguatedPgPRA = Pga.get(topPage)/(1.0*count);						
			if(Pga.size()>25){ ///ASSUMPTION : Top 25 pages are good enough candidate set.
				Pga = pp.prune(a, Pga);	
			}			

			for(long i : Pga.keySet()){ //System.out.println("Pa = "+wikiPageIndexer.getTitle(i));
				double PRA;
				if(memo.containsKey(a+"!@#$"+i)){
					PRA = memo.get(a+"!@#$"+i);
				}
				else{
					int[] PRa = anchorIndexer.getPageCountInPages(a, i);						
					PRA = (1.0*PRa[1]) / PRa[0];
					memo.put(a+"!@#$"+i, PRA);
				}
				// System.out.println("Prior prob Pr(Pa/a) = "+((1.0*PRa[1]) / PRa[0]));
				double[] vote = new double[mentionList.size()];
				for(int j=0;j<mentionList.size();++j){ 
					String b = mentionList.get(j); //System.out.println(" b = "+b);					
					double sum = 0.0;
					vote[j] = 0.0;
					if(b.equalsIgnoreCase(a)){ //skip. Do nothing.
					} else {
						Map<Long,Integer> Pgb = anchorIndexer.getPagesMap(b,0.1); 	//ASSUMPTION : variance restriction = 0.1 ( i.e 10%)
						for(long pgid:Pgb.keySet()){ //System.out.println("Page of b = "+wikiPageIndexer.getTitle(pgid));
							double PRB;
							if(memo.containsKey(mentionList.get(j)+"!@#$"+pgid)){
								PRB = memo.get(mentionList.get(j)+"!@#$"+pgid);
							}
							else{
								int[] PRb = anchorIndexer.getPageCountInPages(mentionList.get(j), pgid);
								PRB = (1.0*PRb[1]) / PRb[0]; 					
								memo.put(mentionList.get(j)+"!@#$"+pgid, PRB);
							} 
							//System.out.println("Prior Pr(Pb/b) = "+PRB);
							if(PRB > 0.1){						
								double rel = inlinkIndex.relatedness(wikiPageIndexer.getTitle(pgid), wikiPageIndexer.getTitle(i));
								System.out.println("Page of b = "+wikiPageIndexer.getTitle(pgid)+", rel = "+rel); 
								if(rel > 0){
									sum += rel * PRB;
								}
							}
							
						}//for each Pb of Pg(b)
						vote[j] = sum ; // vote[j] = sum / Pgb.size(); // For vote normalization.
					}
					relA[PgAiteration] += vote[j];	
	
				}//for each b
				//ASSUMPTION : Linear combination of rel and prior_prob. 
				//ASSUMPTION : alpha score = 0.2			
				linearCombination = relA[PgAiteration]+0.2*PRA;				
				if(linearCombination > maxLinearCombination){
					maxLinearCombination = linearCombination;
					disambiguatedPg = wikiPageIndexer.getTitle(i);
					disambiguatedPgPRA = anchorIndexer.lp(a);
					disambiguatedPgText = a;
				}
				++PgAiteration;				
			}//for each Pga	
			if(disambiguatedPg != null){
				String[] score = new String[2];
				score[0] = disambiguatedPgPRA.toString();
				score[1] = disambiguatedPgText;
				disambiguations.put(disambiguatedPg, score);
				System.out.println("===="+a+" disambiguated to "+disambiguatedPg+". maxLin ="+maxLinearCombination);
			} else {
				System.out.println("===="+a+" NOT disambiguated. ");
			}
			
		}//for each a

		if(disambiguations.size() > 0){			
			HashMap<String, Double> pruned = pruning.coherence(disambiguations);
			Set<String> keys = new HashSet<>();
			keys.addAll(disambiguations.keySet());
			for(String i:keys){
				if(!pruned.containsKey(i)){
					disambiguations.remove(i);
				}
			}
		}//End if disambiguations was not null
		return disambiguations;
	}//end features()

	/*Features for disambiguation by classification
	 * Returns map of mention to String array( member 0 = FreebaseID_of_disambiguated_pg-of-a, 
	 * member 1 = rel_of_a, member 2 = prior_prob_of_a)*/
	public HashMap<String, ArrayList<String>> disambiguationfeatures(String query){

		HashMap<String,Double> memo = new HashMap<>(); 
		HashMap<String, ArrayList<String>> disambiguationFeatures = new HashMap<>(); 		
		ArrayList<String> mentionList = anchorIndexer.mentions(query); // System.out.println("Mention count ="+mentionList.size() + " " + mentionList);
		mentionList = rc.mentionPruning(mentionList, query.toLowerCase());	//mentionList = rc.stopwordPruning(mentionList, query.toLowerCase());
		for(String a:mentionList){ //System.out.println("Mention a ="+a);
			Map<Long,Integer> Pga = anchorIndexer.getPagesMap(a);//System.out.println("Pga count ="+Pga.size());
			double[] relA = new double[Pga.size()];			
			int PgAiteration = 0;
		
			for(long i : Pga.keySet()){ 
				String wikiTitle = wikiPageIndexer.getTitle(i);
				String PgFID = mee.getFreebaseId(wikiTitle); 
				if(PgFID !=null){ //disambiguate mention to only pages in ERD FIDs
					double PRA;
					if(memo.containsKey(a+"!@#$"+i)){
						PRA = memo.get(a+"!@#$"+i);
					}
					else{
						int[] PRa = anchorIndexer.getPageCountInPages(a, i);					
						PRA = (1.0*PRa[1]) / PRa[0];					
						memo.put(a+"!@#$"+i, PRA);
					} // System.out.println("Prior Pr(Pa/a) = "+((1.0*PRa[1]) / PRa[0]));	
	
					double[] vote = new double[mentionList.size()];
					for(int j=0;j<mentionList.size();++j){ 
						String b = mentionList.get(j); //System.out.println(" b = "+b);					
						double sum = 0.0;
						vote[j] = 0.0;
						if(b.equalsIgnoreCase(a)){ //skip. Do nothing.
						} else {
							Map<Long,Integer> Pgb = anchorIndexer.getPagesMap(b, 0.1); 
							for(long pgid:Pgb.keySet()){ //System.out.println("Page of b = "+wikiPageIndexer.getTitle(pgid));
								double PRB;
								if(memo.containsKey(mentionList.get(j)+"!@#$"+pgid)){
									PRB = memo.get(mentionList.get(j)+"!@#$"+pgid);
								}
								else{
									int[] PRb = anchorIndexer.getPageCountInPages(mentionList.get(j), pgid);
									PRB = (1.0*PRb[1]) / PRb[0]; 	
									memo.put(mentionList.get(j)+"!@#$"+pgid, PRB);
								} //System.out.println("Prior Pr(Pb/b) = "+PRB);
								if(PRB > 0.1){						
									double rel = inlinkIndex.relatedness(wikiPageIndexer.getTitle(pgid), wikiPageIndexer.getTitle(i));
									//System.out.println("Page of b = "+wikiPageIndexer.getTitle(pgid)+", rel = "+rel); //double rel=0.1;
									if(rel > 0){
										sum += rel * PRB;
									}
								}
								
							}//for each Pb of Pg(b)
							vote[j] = sum; // vote[j] = sum / Pgb.size();  
						}
						relA[PgAiteration] += vote[j];	//System.out.println("rel["+PgAiteration+"]"+relA[PgAiteration]);
		
					}//for each b

					if(relA[PgAiteration]+memo.get(a+"!@#$"+i) > 0.2){ // to cut down many negative datapoints, prune ones with rrel > 0.2
						if(disambiguationFeatures.containsKey(a)){
							ArrayList<String> disFeatures = disambiguationFeatures.get(a);
							disFeatures.add(PgFID);							
							disFeatures.add(String.valueOf(relA[PgAiteration]));
							disFeatures.add(String.valueOf(memo.get(a+"!@#$"+i)));
							disambiguationFeatures.put(a, disFeatures);
							
						} else {// add new
							ArrayList<String> disFeatures = new ArrayList<String>();// mention=Pga; { relA(Pga), PRA, lp(Pga) }
							disFeatures.add(PgFID);
							disFeatures.add(String.valueOf(relA[PgAiteration]));
							disFeatures.add(String.valueOf(memo.get(a+"!@#$"+i)));
							disambiguationFeatures.put(a, disFeatures); 								
						}
						//System.out.println("mention = "+a+", WikiTitle = "+wikiTitle+", FreebaseID = "+PgFID+ "rel["+PgAiteration+"] = "+relA[PgAiteration]+", PrA = "+memo.get(a+"!@#$"+i));
						//System.out.print(" WikiTitle = "+wikiTitle+", FreebaseID = "+PgFID);
					}//End if cutoff
					
					++PgAiteration;
					}//End if PgFID !=null
				}//for each Pga	
			
		}//for each a
		return disambiguationFeatures;
	}//end disambiguationfeatures()
	
	/*printout disambiguation features in CSV format*/
	public void printFeatures(String query){
		HashMap<String, ArrayList<String>> disambiguationFeatures = disambiguationfeatures(query);
		String output = "";
		int count = 0;
		if(disambiguationFeatures.size()>0){
			for(String j:disambiguationFeatures.keySet()){
				//output +="\n"+j+";";
				for(String feature : disambiguationFeatures.get(j)){
					output += feature+";";
					count++;
					if(count%3==0){
						output += j+";\n";
					}
				}	//+disambiguationFeatures.get(j)+";"+disambiguationFeatures.get(j)[1]+";"+disambiguationFeatures.get(j)[2]+"\n";		
			}
		}
		System.out.println(output);
	}//End printFeatures()
	
	/*Convert to OUTPUT FORMAT of ERD14 Challenge.*/
	public String getResultsFormatted(String quid,String query){		
		HashMap<String, String[]> disambiguations = features(query);
		String output = "";
		if(disambiguations.size()>0){
			for(String j:disambiguations.keySet()){
				if(mee.getFreebaseId(j)!=null){
					output += quid+"\t0\t"+mee.getFreebaseId(j)+"\t"+disambiguations.get(j)[1]+"\t"+disambiguations.get(j)[0]+"\n";
				}
			}
			System.out.println(output);
		}
		anchorIndexer.destroy();
		wikiPageIndexer.destroy();
		inlinkIndex.destroy();
		mee.destroy();
		return output;
	}//End getResultsFormatted()

}//End class
