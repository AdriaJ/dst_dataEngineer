//package tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordCountsForDocsReducer extends Reducer<Text, Text, Text, Text> {

	public WordCountsForDocsReducer() {
	}

	private static Text OUT_KEY = new Text();
	private static Text OUT_VALUE = new Text();

	/**
	 * ENTREE: une paire au format <documentName,["word1=n1","word2=n2",...]>
	 * 
	 * SORTIE: un ensemble de paires au format <wordk@documentName,freqWordInDoc/nbWordInDoc>
	 * exemple : <"word1@a.txt, 3/13">, <"word2@a.txt, 5/13">, ...
	 * 
	 * @param key
	 *            la clé du mapper
	 * @param values
	 *            un tableau de "word1=n1"
	 * @param context
	 *            le contexte d'application
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sumOfWordsInDocument = 0;
		Map<String, Integer> tempCounter = new HashMap<String, Integer>();
		
		for (org.apache.hadoop.io.Text val : values) {
			String[] freqs = val.toString().split("=");
			int number = Integer.parseInt(freqs[1]);
			tempCounter.put(freqs[0], number);
			sumOfWordsInDocument = sumOfWordsInDocument + number;
		}
		
//		Set<Entry<String, Integer>> setTC = tempCounter.entrySet();
//		Iterator<Entry<String, Integer>> it = setTC.iterator();
		for(Entry<String, Integer> e : tempCounter.entrySet()){
//			Entry<String, Integer> e = it.next();
			OUT_KEY.set(e.getKey() + "@" + key.toString());
			OUT_VALUE.set(Integer.toString(e.getValue()) +'/'+ Integer.toString(sumOfWordsInDocument));
//			OUT_VALUE.set(Double.toString((double) e.getValue()/sumOfWordsInDocument));
			context.write(OUT_KEY,OUT_VALUE);
		}
		

		// Une premiere boucle pour (1) stocker dans une structure temporaire le nombe d'occurrence de chaque mots
		// dans le document et (2) compter le nombre total de mots dans le document


		// On parcourt la structure temporaire et on emet les paires

				
	}
}
