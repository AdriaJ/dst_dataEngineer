//package tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordsInCorpusTFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public WordsInCorpusTFIDFMapper() {
	}

	// Reuse writables
	private static Text INTERM_KEY = new Text();
	private static Text INTERM_VALUE = new Text();

	/**
	 *
	 * 
	 * SORTIE : une paire du type <word,documentName=freqWordIndoc/nBWordInDoc 
	 * Exemple : <toto, book.txt=3/1500>
	 * 
	 * @param key
	 *            est l'offeset de la ligne considérée dans le fichier;
	 * @param value
	 *            est la ligne du fichier pointée par l'offeset
	 *			  Son format est wordk@documentName \t freqWordInDoc/nbWordInDoc
	 *			  Exemple :	toto@book.txt \t 3/1500
	 * @param context
	 *            le contexte du job
	 * 
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			String[] val = itr.nextToken().split("@"); 
			INTERM_KEY.set(val[0]);
			INTERM_VALUE.set(val[1]+'='+itr.nextToken());
		}
		context.write(INTERM_KEY, INTERM_VALUE);

	}
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while(context.nextKeyValue()){
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}

}
