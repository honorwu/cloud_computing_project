import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job1Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String data[] = value.toString().split(",");

		context.write(new Text(data[3]), new LongWritable(1));
		context.write(new Text(data[4]), new LongWritable(1));
	}
}
