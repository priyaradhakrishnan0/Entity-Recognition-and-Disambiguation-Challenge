package version1;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import Variables.Variables;
/*Class : API to call server implementation of Allan Ritter's short text POS tagging and chunking algorithm*/
public class RitterChunks {

	public static void main(String[] args) {

		RitterChunks rc = new RitterChunks();
		try {
			System.out.println(rc.chunks("The act band"));
			System.out.println(rc.getEntities("Google Search is going the best search engine worldwide"));
			System.out.println(rc.getPOS("Google Search is going the best search engine worldwide"));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*Input list of mentions identified in the given tweet.
	 * Removes mentions that do not contain at least one word of POS tag NN. 
	 * Returns the cleaned mention list.*/
	public ArrayList<String> mentionPruning(ArrayList<String> mentions,String tweet){
		HashMap<String, String> posTags = getPOS(tweet);
		//System.out.println(posTags);
		for(int i=0; i<mentions.size();i++){
			boolean flag = true;
			for(String j : mentions.get(i).split(" ")){
				//System.out.println(j);
				if(posTags.get(j).startsWith("NN")){
					flag = false;
					break;
				}
			}
			if(flag){
				mentions.remove(i);
				i--;
			}
		}
		return mentions;
	}
	
	/*Input list of mentions identified in the given tweet.
	 * Removes mentions that contain only stopwords and returns the cleaned mention list.
	 * Uses JMLR stopword list available at http://jmlr.org/papers/volume5/lewis04a/a11-smart-stop-list/english.stop */
	public ArrayList<String> stopwordPruning(ArrayList<String> mentions,String tweet){
		String[] stop = 
			{"a","a's","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","ain't","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren't","around","as","aside","ask","asking","associated","at","available","away","awfully","b","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c","c'mon","c's","came","can","can't","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","course","currently","d","definitely","described","despite","did","didn't","different","do","does","doesn't","doing","don't","done","down","downwards","during","e","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","g","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","h","had","hadn't","happens","hardly","has","hasn't","have","haven't","having","he","he's","hello","help","hence","her","here","here's","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i","i'd","i'll","i'm","i've","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","isn't","it","it'd","it'll","it's","its","itself","j","just","k","keep","keeps","kept","know","knows","known","l","last","lately","later","latter","latterly","least","less","lest","let","let's","like","liked","likely","little","look","looking","looks","ltd","m","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","n","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","o","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","p","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","q","que","quite","qv","r","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","s","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","shouldn't","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t","t's","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that's","thats","the","their","theirs","them","themselves","then","thence","there","there's","thereafter","thereby","therefore","therein","theres","thereupon","these","they","they'd","they'll","they're","they've","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","u","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","uucp","v","value","various","very","via","viz","vs","w","want","wants","was","wasn't","way","we","we'd","we'll","we're","we've","welcome","well","went","were","weren't","what","what's","whatever","when","whence","whenever","where","where's","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","who's","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","won't","wonder","would","would","wouldn't","x","y","yes","yet","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","z","zero"};
		HashSet<String> stopwords = new HashSet<>(Arrays.asList(stop));
		for(int i=0; i<mentions.size();i++){
			boolean flag = true;
			for(String j : mentions.get(i).split(" ")){
				//System.out.println(j);
				if(!stopwords.contains(j.trim().toLowerCase())){
					flag = false;
					break;
				}
			}
			if(flag){
				mentions.remove(i);
				i--;
			}
		}
		return mentions;
	}
	
	/*Runs Ritter POS tagger on input string.
	 * Returns a map of word to POS tag in the POS output*/
	public HashMap<String, String> getPOS(String tweet){
		HashMap<String, String> pos = new HashMap<>();
		try {
			tweet=tweet.replaceAll("#","");
			String value=parse(tweet); 	//System.out.println("RC parse output :"+value);
			StringTokenizer st = new StringTokenizer(value," ",true);
			while (st.hasMoreTokens()) {
				String tokens[] = st.nextToken().split("\\/");
				if(tokens.length==4)
				{
					pos.put(tokens[0], tokens[2]);
				}
			}
		}catch(Exception e){

		}
		return pos;
	}//End getPOS()
	
	/*Runs Ritter POS tagger on input string.
	 * Returns array of entity mention strings in the POS output*/
	public ArrayList<String> getEntities(String tweet){
		String part="";
		ArrayList<String> entities=new ArrayList<String>();
		try {
			tweet=tweet.replaceAll("#","");
			String value=parse(tweet);
			//System.out.println(value);
			StringTokenizer st = new StringTokenizer(value," ",true);
			int count=0;
			while (st.hasMoreTokens()) {
				String tokens[] = st.nextToken().split("\\/");
				if(tokens.length==4)
				{
					//System.out.println(tokens[3]);
					if((tokens[1].startsWith("B-ENTITY")))
					{
						if(count == 0){
							count=1;
							part+=tokens[0]+" ";}
						else{
							entities.add(part.trim());
							part =tokens[0]+" ";
						}
					}
					else if(tokens[3].startsWith("I-")){
						part+=tokens[0]+" ";
					}
					else{
						if(count==1)
						{
							count=0;
							entities.add(part.trim());
							part="";
						}
					}
				}
			}
			if(count==1)
			{
				count=0;
				entities.add(part.trim());
				part="";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return entities;
	}//End getEntities

	/*Runs Ritter POS tagger on input string.
	 * Returns array of string chunks in the POS output*/
	public ArrayList<String> chunks(String tweet)
	{
		String part="";
		ArrayList<String> chunks=new ArrayList<String>();
		try {
			tweet=tweet.replaceAll("#","");
			String value=parse(tweet); 	//System.out.println(value);
			StringTokenizer st = new StringTokenizer(value," ",true);
			int count=0;
			while (st.hasMoreTokens()) {
				String tokens[] = st.nextToken().split("\\/");
				if(tokens.length==4)
				{ 
					if((tokens[3].startsWith("B-")))
					{
						if(count == 0){
							count=1;
							part+=tokens[0]+" ";}
						else{
							chunks.add(part.trim());
							part =tokens[0]+" ";
						}
					}
					else if(tokens[3].startsWith("I-")){
						part+=tokens[0]+" ";
					}
					else if(tokens[1].startsWith("B-ENTITY")){ //Begin Entity
						if(count==1)
						{
							count=0;
							chunks.add(part.trim());
							part="";
						}
						count=1;
						part+=tokens[0]+" ";
					}
					else if(tokens[1].startsWith("I-ENTITY")){ //Intermediate Entity
						part+=tokens[0]+" ";
						count = 1;
					}
					else{
						if(count==1)
						{
							count=0;
							chunks.add(part.trim());
							part="";
						}

					}
				}
			}
			if(count==1)
			{
				count=0;
				chunks.add(part.trim());
				part="";
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return chunks;
	}

	/*Contacts RitterChunks server.
	 * Returns output of running Alan Ritter's POS tagger on the query string*/
	public String parse(String Tweet) throws IOException {
		String parse_string="";
		try {
			URL url = new URL(Variables.ritterChunks);
			HttpURLConnection uc = (HttpURLConnection) url.openConnection();
			uc.setRequestMethod("POST");
			uc.setDoOutput(true);
			DataOutputStream wr = new DataOutputStream(uc.getOutputStream());
			wr.writeBytes("tweet="+Tweet);
			wr.flush();
			int rspCode = uc.getResponseCode();
			if (rspCode == 200) {
				InputStream is = uc.getInputStream();
				InputStreamReader isr = new InputStreamReader(is);
				BufferedReader br = new BufferedReader(isr);
				parse_string = br.readLine();
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return parse_string;

	}
}
