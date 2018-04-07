package bigdata.avarageWordLength;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvarageLengthReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	
	/**
	 * dopo la fase di shuffle & sort vengono eseguite tante reduce quante sono le chiavi, qui la reduce fa solamente la media della lista associata alla chiave key
	 */
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		int total = 0, sum = 0;
		
		for(IntWritable currentValue : values) {
			sum += currentValue.get();
			total++;
		}
		
		context.write(key, new DoubleWritable(((double)sum)/ total));				
	}

}
