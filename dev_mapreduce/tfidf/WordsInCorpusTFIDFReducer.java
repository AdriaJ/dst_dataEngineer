//package tfidf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordsInCorpusTFIDFReducer extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private static Text OUT_CLE = new Text();
	private static Text OUT_VALEUR = new Text();

	public WordsInCorpusTFIDFReducer() {
	}

	/**
	 * ENTREE: une paire du type  <word, ["doc1=n1/N1", "doc2=n2/N2"]>
	 * 
	 * SORTIE: une paire de la forme <word@fileName,  [d/D, n/N, TF-IDF]> avec :
	 *				- d est le nombre de documents qui contient word
	 *				- D est le nombre total de Documents
	 *				- n est le nombre d'occurrences de word dans fileName
	 *				- N est le nombre de mots dans fileName
	 *				- TF-IDF est le tf-idf de wordname
	 * @param key
	 *            est la clé retournée par le mapper
	 * @param values
	 *            est  un tableau de la forme ["doc1=n1/N1", "doc2=n2/N2", ...]
	 * @param context
	 *            contient le contexte d'exécution du job
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// Pour récupérer le nombre de documents dans le corpus D
		int numberOfDocsInCorpus = context.getConfiguration().getInt("docsInCorpus", 1);
		System.out.println(numberOfDocsInCorpus);

		// Nombre total d'occurences du mot et comptabilisation du nombre de documents avec le mot en question 
		int numberOfDocsInCorpusWithKey = 0;
		Map<String, String> tempFrequencies = new HashMap<String, String>(); // Stockera des éléments du type "doc1" => "n1/N1"
		for (Text val : values) {
			String[] instance = val.toString().split("=");
			tempFrequencies.put(instance[0], instance[1]);
			numberOfDocsInCorpusWithKey = numberOfDocsInCorpusWithKey+1;
		}

		for (Entry<String, String> entry : tempFrequencies.entrySet()) {

			// calcul du tf
			double tf ;
			
			String[] numbs = entry.getValue().split("/");
			tf =(double) Double.valueOf(numbs[0])/Double.valueOf(numbs[1]);
			
			// Calcul de l'idf d/D
			double idf ;
			idf = (double) numberOfDocsInCorpusWithKey/numberOfDocsInCorpus;

			// calcul du tf-idf
			double tfIdf ;
			tfIdf = tf * Math.log10((double) 1/idf);

			// Création de la sortie selon le format spécifié

			OUT_CLE.set(key.toString()+"@"+entry.getKey());
			OUT_VALEUR.set(DF.format(tfIdf));
			context.write(OUT_CLE, OUT_VALEUR);
		}
	}
}
