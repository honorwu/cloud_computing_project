import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Q12 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Q12");
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

        JavaDStream<String> line = lines.map(new Function<ConsumerRecord<String, String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                return stringStringConsumerRecord.value();
            }
        });

        JavaDStream<String> filter = line.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                String[] data = s.split(",");
                if (data.length < 9) {
                    return false;
                }
                return true;
            }
        });

        JavaPairDStream<String, String> pair = filter.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] data = s.split(",");
                return new Tuple2<>(data[1], data[8]+",1");
            }
        });

        JavaPairDStream<String, String> reduce = pair.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                String []data1 = s.split(",");
                String []data2 = s2.split(",");

                Double sum = Double.parseDouble(data1[0]) + Double.parseDouble(data2[0]);
                Integer count = Integer.parseInt(data1[1]) + Integer.parseInt(data2[1]);
                return sum.toString() + "," + count.toString();
            }
        });

        JavaPairDStream<Double, String> swap  = reduce.mapToPair(new PairFunction<Tuple2<String, String>, Double, String>() {
            @Override
            public Tuple2<Double, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String [] data = stringStringTuple2._2.split(",");
                return new Tuple2<>(Double.parseDouble(data[0])/Integer.parseInt(data[1]), stringStringTuple2._1);
            }
        });

        JavaPairDStream<Double, String> sort = swap.transformToPair(new Function<JavaPairRDD<Double, String>, JavaPairRDD<Double, String>>() {
            @Override
            public JavaPairRDD<Double, String> call(JavaPairRDD<Double, String> doubleStringJavaPairRDD) throws Exception {
                return doubleStringJavaPairRDD.sortByKey();
            }
        });

        sort.print();
        sort.dstream().saveAsTextFiles("/Task2/Q12/","");

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
