package bigdata.invertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.Writable;

public class IterableWritable<T extends Writable> implements Writable {
	

	private Iterable<T> iterable;
	
	public IterableWritable(Iterable<T> it) {
		this.setIterable(it);
	}

	public void readFields(DataInput in) throws IOException {
		System.out.println("\n\n\n");
		System.out.println("readFields");
		System.out.println("\n\n\n");
		
	}

	public void write(DataOutput out) throws IOException {
		getIterable().forEach(e -> {
			try {
				e.write(out);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		});
	}

	public Iterable<T> getIterable() {
		return iterable;
	}

	public void setIterable(Iterable<T> iterable) {
		this.iterable = iterable;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("(");
		
		iterable.forEach(e -> builder.append(e.toString() + "  "));
		
		return builder.toString() + ")";
	}
	
	
	
}
