package bigdata.wordCount;
import org.apache.hadoop.conf.Configuration;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration(), "WordCount");

		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		
		/**
		 * Users can optionally specify a combiner, via Job.setCombinerClass(Class), to perform local aggregation of 
		 * the intermediate outputs, which helps to cut down the amount of data transferred from the Mapper to the Reducer
		 */
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);

		/**
		 * Get the list of input Paths for the map-reduce job.
		 * infatti quando si va a scrivere il comando di esecuzione, il primo parametro è sempre quello che specifica dov'è il file
		 * di testo => input/words.txt e qui giustamente è args[0].
		 */
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}
