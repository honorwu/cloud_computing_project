import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q22 {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage:Q22 <in> <out>");
			System.exit(2);
		}

		Job job1 = new Job(conf, "job1");
		job1.setJarByClass(Q22.class);

		job1.setMapperClass(Job1Mapper.class);
		job1.setCombinerClass(Job1Combiner.class);
		job1.setReducerClass(Job1Reducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job1, new Path("/temp/Q22"));

		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "job2");
		job2.setJarByClass(Q22.class);

		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job2, new Path("/temp/Q22"));
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
