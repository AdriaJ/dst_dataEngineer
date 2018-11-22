package tfidf;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Permet de lancer les 3 jobs à la suite
 * 
 *
 */
public class Driver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();

		try {
			// Add all the join main classes
			pgd.addClass("tf-idf-1", WordFrequenceInDoc.class,
					"TF-IDF 1 --- Frequence des mots par document");
			pgd.addClass("tf-idf-2", WordCountsForDocs.class,
					"TF-IDF 2 --- Nombre de mots par document");
			pgd.addClass("tf-idf-3", WordsInCorpusTFIDF.class,
					"TF-IDF 3 --- Nombre de mots dans le corpus et calcul du TF-IDF");

			// Lance le programme
			pgd.driver(args);

			// Succes
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
