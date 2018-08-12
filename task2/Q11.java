import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Q11 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Q11");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(Integer.parseInt(args[0])));

        Long currentTime = System.currentTimeMillis();
        String groupId = currentTime.toString();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");

        String topics = "1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008";

        JavaInputDStream<ConsumerRecord<String, String>> lines =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topics.split(",")), kafkaParams)
                );

        JavaDStream<String> airport = lines.flatMap(
                new FlatMapFunction<ConsumerRecord<String, String>, String>() {
                    public Iterator<String> call(ConsumerRecord<String, String> x) {
                        String [] data = x.value().toString().split(",");
                        ArrayList<String> airports = new ArrayList<String>();
                        airports.add(data[3]);
                        airports.add(data[4]);
                        return airports.iterator();
                    }
                });

        JavaPairDStream<String, Integer> counts = airport
                .mapToPair(x -> new Tuple2<String, Integer>(x, 1))
                .reduceByKey((x, y) -> x + y);

        JavaPairDStream<Integer, String> swap = counts.mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1));

        JavaPairDStream<Integer, String> sort = swap.transformToPair(
                new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
                    @Override
                    public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> integerStringJavaPairRDD) throws Exception {
                        return integerStringJavaPairRDD.sortByKey(false);
                    }
                });

        sort.print();
        sort.dstream().saveAsTextFiles("/Task2/Q11/","");

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
