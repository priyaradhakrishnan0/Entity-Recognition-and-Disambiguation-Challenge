package version1;

import java.util.HashMap;

public class Pruning {

	/*Calculate coherence between its candidate annotation(Pages) of one mention with 
	 * candidate annotations of the other anchors in the query sentence. This is combined with
	 * link probability to get the pruning score.
	 * Returns map of mention to pruning score.*/
	public HashMap<String, Double> coherence(HashMap<String, String[]> disambiguations){
		InlinkIndex inlinkIndex = new InlinkIndex();
		HashMap<String, Double> coherence = new HashMap<>();
		for(String i:disambiguations.keySet()){
			double rel = 0;
			int length = 0;
			for(String j:disambiguations.keySet()){
				if(i.equals(j)){
					continue;
				}
				length ++;
				rel += inlinkIndex.relatedness(i, j);
			}//End for b
			if(length==0){
				rel = .3;
				length = 1;
			}
			double score = Double.parseDouble(disambiguations.get(i)[0]);
			//System.out.println(i + " " + rel/length + " " + .1*score + " " + (rel/length + .1*score));
			//ASSUMPTION : Linear combination of coherence and lp. 
			//ASSUMPTION : Beta score = 0.1  
			if((rel/length + .1*score) > .05){ ///ASSUMPTION :  Pruning score threshold = 0.05
				coherence.put(i, rel/length + .1*score);
			}
		}//End for a
		//System.out.println("Coherence "+ coherence);
		inlinkIndex.destroy();
		return coherence;
	}//End coherence()

}//End class
