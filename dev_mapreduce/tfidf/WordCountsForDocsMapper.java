//package tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordCountsForDocsMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	public WordCountsForDocsMapper() {
	}

	// Reuse writables
	private static Text INTERM_KEY = new Text();
	private static Text INTERM_VALUE = new Text();

	/**
	 *
	 * 
	 * SORTIE:  un ensemble de paires du type <documentName, mot=freq>
	 * 
	 * @param key
	 *            est l'offset de la ligne du fichier considéré;
	 * @param value
	 *            est la ligne pointée par le fichier au format mot@documentName \t freq
	 * @param context
				  est le contexte d'application
	 */
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		
		while (itr.hasMoreTokens()) {
			String[] name = itr.nextToken().split("@");
			String freq = itr.nextToken();
			INTERM_KEY.set(name[1]);
			INTERM_VALUE.set(name[0]+"="+freq);
			context.write(INTERM_KEY , INTERM_VALUE); 
		}
	}
	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()){
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

}
