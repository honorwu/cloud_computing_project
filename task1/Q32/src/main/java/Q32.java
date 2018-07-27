import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Q32 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:Q32 <in> <out>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "job1");
		job1.setJarByClass(Q32.class);

		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
