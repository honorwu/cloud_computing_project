import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Job1Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;

		Iterator<LongWritable> iter = values.iterator();
		while (iter.hasNext()) {
			sum += iter.next().get();
		}

		context.write(key, new LongWritable(sum));
	}
}