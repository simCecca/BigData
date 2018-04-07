package bigdata.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIndexReducer extends Reducer<Text, IntWritable, Text, IterableWritable<IntWritable>> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> val,
			Reducer<Text, IntWritable, Text, IterableWritable<IntWritable>>.Context ctx)
			throws IOException, InterruptedException {
		
		
		IterableWritable<IntWritable> it = new IterableWritable<>(val);
		
		ctx.write(key, it);
		
		
	}
	
	
	

}
