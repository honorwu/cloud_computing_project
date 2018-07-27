import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String data[] = value.toString().split(",");

		if (data.length < 9) {
			return;
		}

		context.write(new Text(data[0] + "," + data[3] + "," + data[4]),
				new Text(data[1] + "," + data[2] + "," + data[5] + "," + data[8]));
	}
}
