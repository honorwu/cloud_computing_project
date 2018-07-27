import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String data[] = value.toString().split(",");

		context.write(new Text(data[0]+","+data[1]), new Text(data[2] + "_" + data[3]));
	}
}
