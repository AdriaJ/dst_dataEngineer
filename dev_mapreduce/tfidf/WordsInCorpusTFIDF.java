
//package tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordsInCorpusTFIDF extends Configured implements Tool {

	/**
	 * Configure le job MR
	 * 
	 * @param args
	 * @throws Exception
	 */

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.println("Usage: tf-idf-3 <doc-input> <tf-idf-2-output> <output>");
			System.exit(-1);
		}

		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Compte les documents dans le corpus et calcule le TF-IDF");
		conf = job.getConfiguration();

		job.setJarByClass(WordsInCorpusTFIDF.class);
		job.setMapperClass(WordsInCorpusTFIDFMapper.class);
		job.setReducerClass(WordsInCorpusTFIDFReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// Pour compter le nombre de documents dans le corpus
		Path inputPath = new Path(args[0]);
		FileSystem fs = inputPath.getFileSystem(conf);
		FileStatus[] matches = fs.globStatus(inputPath);
		if (matches == null || matches.length == 0)
			throw new IOException("Le répertoire n'existe pas : " + inputPath);

		int docsInCorpus = 0;
		for (FileStatus globStat : matches) {
			if (globStat.isDirectory()) {
				docsInCorpus += fs.listStatus(globStat.getPath()).length;
			} else {
				docsInCorpus += 1;
			}
		System.out.println("Docs in corpus :" + docsInCorpus);
		}

		conf.setInt("docsInCorpus", docsInCorpus);

		FileSystem fs2 = FileSystem.newInstance(getConf());
		if (fs2.exists(new Path(args[2]))) {
			fs2.delete(new Path(args[2]), true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Main driver
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		WordsInCorpusTFIDF exempleWIC = new WordsInCorpusTFIDF();
		int res = ToolRunner.run(exempleWIC, args);
		System.exit(res);
	}
}
