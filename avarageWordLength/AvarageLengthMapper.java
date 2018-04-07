package bigdata.avarageWordLength;

import org.apache.hadoop.mapreduce.Mapper;

import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

// Class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
public class AvarageLengthMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private IntWritable valueOutput = new IntWritable();
	private Text currentWordForReducer = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		StringTokenizer token = new StringTokenizer(line);
		while(token.hasMoreTokens()) {
			String currentTemporaryWord = token.nextToken();
			currentWordForReducer.set(currentTemporaryWord.substring(0,1));
			valueOutput.set(currentTemporaryWord.length());
			context.write(currentWordForReducer, valueOutput);
		}
		
	}

}
