package bigdata.invertedIndex;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Inverted Index
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
    	
    	Job job = new Job(new Configuration(), "inv index");
    	
    	job.setJarByClass(App.class);
    	
    	job.setMapperClass(InvIndexMapper.class);
    	
    	job.setReducerClass(InvIndexReducer.class);
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	job.setOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(IntWritable.class);
    	
    	job.setOutputValueClass(IterableWritable.class);
    	
    	job.waitForCompletion(true);
    	
        
    }
}
