import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Q21 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Q21");
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
                if (data.length < 7) {
                    return false;
                }
                return true;
            }
        });

        JavaPairDStream<String, String> pair = filter.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] data = s.split(",");
                return new Tuple2<>(data[3] + "," + data[1], data[6]+",1");
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

        JavaPairDStream<String, String> swap  = reduce.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String [] airportCarrier = stringStringTuple2._1.split(",");
                String [] data = stringStringTuple2._2.split(",");
                Double result = Double.parseDouble(data[0])/Integer.parseInt(data[1]);
                return new Tuple2<>(airportCarrier[0], airportCarrier[1] + "," + result.toString());
            }
        });

        JavaPairDStream<String, Iterable<String>> group = swap.groupByKey();

        JavaPairDStream<String, Iterable<String>> result = group.mapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> strings) throws Exception {
                TreeMap<Double, String> treeMap = new TreeMap<Double, String>();
                for (String s : strings) {
                    String [] data = s.split(",");
                    treeMap.put(Double.parseDouble(data[1]), data[0]);
                    if (treeMap.size() > 10) {
                        treeMap.remove(treeMap.lastKey());
                    }
                }
                List<String> returnList = new ArrayList<String>();
                Iterator iterator = treeMap.keySet().iterator();
                while (iterator.hasNext()) {
                    Double delay = (Double) iterator.next();
                    String carrier = treeMap.get(delay);
                    returnList.add(carrier + "," + delay.toString());
                }
                return returnList;
            }
        });


        result.print();
        result.dstream().saveAsTextFiles("/Task2/Q21/","");

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
