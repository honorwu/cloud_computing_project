import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Job1Reducer extends Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Double sum = 0d;
		Integer count = 0;

		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			String data[] = iter.next().toString().split(",");
			sum += Double.parseDouble(data[0]);
			count += Integer.parseInt(data[1]);
		}

		Double result = sum / count;

		context.write(NullWritable.get(), new Text(key.toString() + "," + result.toString()));
	}
}