import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

public class Job1Reducer extends Reducer<Text, Text, NullWritable, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<Double, String> am = new TreeMap<Double, String>();
		TreeMap<Double, String> pm = new TreeMap<Double, String>();

		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			String v = iter.next().toString();
			String[] data = v.split(",");

			Double depTime = Double.parseDouble(data[2]);
			Double delayMinute = Double.parseDouble(data[3]);

			if (depTime < 1200) {
				// am
				am.put(delayMinute, v);
				if (am.size() > 1) {
					am.remove(am.lastKey());
				}
			} else if (depTime > 1200){
				// pm
				pm.put(delayMinute, v);
				if (pm.size() > 1) {
					pm.remove(pm.lastKey());
				}
			}
		}

		if (!am.isEmpty()) {
			context.write(NullWritable.get(), new Text(key.toString() + "," + am.get(am.firstKey())));
		}

		if (!pm.isEmpty()) {
			context.write(NullWritable.get(), new Text(key.toString() + "," + pm.get(pm.firstKey())));
		}
	}
}