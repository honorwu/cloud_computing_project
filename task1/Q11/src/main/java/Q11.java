import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q11 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:Q11 <in> <out>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "Q11");
		job1.setJarByClass(Q11.class);

		job1.setMapperClass(Job1Mapper.class);
		job1.setCombinerClass(Job1Reducer.class);
		job1.setReducerClass(Job1Reducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
