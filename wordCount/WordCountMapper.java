package bigdata.wordCount;


import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * allora, l'InputFormats org.apache.hadoop.mapreduce.InputFormat<K,V>, sono tipicamente sottoclassi di FileInputFormat, i quali dividono
 * il file di input (FileInputFormat.addInputPath in WordCount.java) e lo splittano con InputSplits in base alla dimensione del file (in byte)
 * il lower bownd della dimensione degli split può essere specificata in  mapreduce.input.fileinputformat.split.minsize, mentre l'upper bound
 * degli input è dato dalla dimensione dei blocchi del file system.
 * 
 * il framework mapreduce esegue una map per ogni InputSplit generato da InputFormat per quel job. 
 * il flusso di esecuzione di queste chiamate adottato dal framework è: la prima chiamata è all setup(org.apache.hadoop.mapreduce.Mapper.Context)
 * dopo di che viene richiamato  map(Object, Object, org.apache.hadoop.mapreduce.Mapper.Context) per ogni coppia key/value splittata
 * ed in fine viene richiamata la cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
 * @author simone
 *
 */


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		private static final IntWritable one = new IntWritable(1);
		private Text word =  new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}

}
