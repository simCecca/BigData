package bigdata.invertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	
		
		String row = value.toString();
		
		System.out.println(row);
		
		IntWritable rowNr = new IntWritable(Integer.parseInt(row.split("\t")[0]));
		
		
		String[] elems = row.split("\t")[1].split(" ");
		
		for (String elem : elems)
			context.write(new Text(elem), rowNr);
		
	}
	
	

}
