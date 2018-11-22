//package tfidf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Permet de compter le nombre d'occurrences de chaque mot par document
 *
 */
public class WordFrequenceInDoc extends Configured implements Tool {

	/**
	 * Configure le job MR
	 * 
	 * @param args
	 * @throws Exception
	 */
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: tf-idf-1 <doc-input> <tf-idf-1-output>");
			System.exit(-1);
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Frequence des mots par document");

		job.setJarByClass(WordFrequenceInDoc.class);
		job.setMapperClass(WordFrequenceInDocMapper.class);
		job.setReducerClass(WordFrequenceInDocReducer.class);
		job.setCombinerClass(WordFrequenceInDocReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}
	

	public static void main(String[] args) throws Exception {

        WordFrequenceInDoc exempleDriver = new WordFrequenceInDoc();
        int res = ToolRunner.run(exempleDriver, args);
        System.exit(res);
	}

}
