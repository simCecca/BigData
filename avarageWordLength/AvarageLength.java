package bigdata.avarageWordLength;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;


/**
 *
 *
 */
public class AvarageLength 
{
    public static void main( String[] args ) throws Exception
    {
        Job job = new Job(new Configuration(), "AvarageLength");
        job.setJarByClass(AvarageLength.class);
        job.setMapperClass(AvarageLengthMapper.class);
        
        //job.setCombinerClass(AvarageLengthReduce.class);
        job.setReducerClass(AvarageLengthReduce.class);
        
        FileInputFormat.addInputPath(job,  new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.waitForCompletion(true);
        
        
    }
}
