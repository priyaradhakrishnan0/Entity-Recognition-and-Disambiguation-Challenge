package version1;
/*Class : Remove pages with less freq_of_occurance.*/
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

public class PruneBottomPages {

	public static void main(String[] args) {
		// dummy
	}

    /*Inputs map of Pageid to freq_of_PageId.
     * Returns map ranked in the decreasing order of freq_of_PageId.
     */
	<K,V extends Comparable<? super V>>
	SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
		SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
				new Comparator<Map.Entry<K,V>>() {
					@Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
						if(e2.getValue().compareTo(e1.getValue())==0 || e2.getValue().compareTo(e1.getValue())>0)
							return 1;
						else
							return -1;
					}
				}
				);
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

    /*Inputs map of Pageid to freq_of_PageId.
     * Ranks the map by the decreasing order of freq_of_PageId.
     * Retuns top 25 of the ranked Map.*/
	public HashMap<Long, Integer> prune(String a,Map<Long, Integer> pga){
		HashMap<Long, Integer> pruned = new HashMap<>();
		int count = 0;
		for(Entry<Long, Integer> i:entriesSortedByValues(pga)){
			pruned.put(i.getKey(), i.getValue());
			if(count++ > 25)
				break;
		}
		return pruned;
	}//End prune()
	
    /*Inputs map of Pageid to freq_of_PageId.
     * Ranks the map by the decreasing order of freq_of_PageId.
     * Returns PageId of the top entry in the ranked Map.*/
	public Long getTop(String a,Map<Long, Integer> pga){
		for(Entry<Long, Integer> i:entriesSortedByValues(pga)){
			return i.getKey();
		}
		return null;
	}//End getTop()

}
