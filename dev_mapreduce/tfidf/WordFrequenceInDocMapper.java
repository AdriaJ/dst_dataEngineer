//package tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * WordFrequenceInDocMapper est la première étape pour calculer le TFIDF
 */
public class WordFrequenceInDocMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public WordFrequenceInDocMapper() {
	}


	// Compile all the words using regex
	private static Pattern pattern = Pattern.compile("\\w+");

	// Reuse writeables
	private static Text INTERM_KEY = new Text();
	private static IntWritable ONE = new IntWritable(1);
	private static StringBuilder valueBuilder = new StringBuilder();

	/**
	 * SORTIE DU MAP: un ensemble de paires du type <mot@documentName,1>
	 * 
	 * @param key
	 *            est l'offset de la ligne considérée
	 * @param value
	 *           est la ligne du fichier
	 * @param context
	 *            contient le contexte du job
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		while (itr.hasMoreTokens()) {
			INTERM_KEY.set(itr.nextToken().toLowerCase().replaceAll("[^a-zéèàêî0-9 ]", "")+"@"+fileName);
			context.write(INTERM_KEY, ONE);
			}
		

		// Pensez à mettre la ligne en minuscule aavnt de la traiter
				
		// La commande pour récupérer le nom du fichier considérer est :
		// String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				
	
	}
	public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    while(context.nextKeyValue()){
	        map(context.getCurrentKey(), context.getCurrentValue(), context);
	    }
	    cleanup(context);
	}

}
