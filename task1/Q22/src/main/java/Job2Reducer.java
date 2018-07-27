import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class Job2Reducer extends Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		TreeMap<Double, String> treeMap = new TreeMap<Double, String>();
		while (iter.hasNext()) {
			String [] data = iter.next().toString().split("_");
			treeMap.put(Double.parseDouble(data[1]),data[0]);

			if (treeMap.size() > 10) {
				treeMap.remove(treeMap.lastKey());
			}
		}

		Iterator iterator = treeMap.keySet().iterator();
		while (iterator.hasNext()) {
			Double delay = (Double) iterator.next();
			String carrier = treeMap.get(delay);
			context.write(NullWritable.get(), new Text(key + "," + carrier + "," + delay.toString()));
		}
	}
}