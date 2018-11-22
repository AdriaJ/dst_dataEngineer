//package tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordFrequenceInDocReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private static IntWritable COUNT = new IntWritable();

	public WordFrequenceInDocReducer() {
	}

	/**
	 * ENTREE:  recoit une paire de type <mot@fileName,[1,1,...]>
	 * 
	 * SORTIE:  une paire du type <mot@fileName, n> où n est la taille du tableau
	 * 
	 * @param key
	 *            est la clé émise par le mapper
	 * @param values
	 *            est le tableau de 1
	 * @param context
	 *            contient le context d'exécution
	 * 
	 */
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum +=val.get();
		}
		COUNT.set(sum);
		context.write(key, COUNT);
		
	}
}
