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

public class Q32 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Q32");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(Integer.parseInt(args[0])));

        Long currentTime = System.currentTimeMillis();
        String groupId = currentTime.toString();

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "earliest");

        String topics = "2008";

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
                return new Tuple2<>(data[0] + "," + data[3] + "," + data[4],
                        data[1] + "," + data[2] + "," + data[5] + "," + data[6] + "," + data[8]);
            }
        });

        JavaPairDStream<String, Iterable<String>> group = pair.groupByKey();

        JavaPairDStream<String, Iterable<String>> result = group.mapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> strings) throws Exception {
                TreeMap<Double, String> am = new TreeMap<Double, String>();
                TreeMap<Double, String> pm = new TreeMap<Double, String>();

                for (String s : strings) {
                    String [] data = s.split(",");
                    Double depTime = Double.parseDouble(data[2]);
                    Double delayMinute = Double.parseDouble(data[4]);

                    if (depTime < 1200) {
                        // am
                        am.put(delayMinute, s);
                        if (am.size() > 1) {
                            am.remove(am.lastKey());
                        }
                    } else if (depTime > 1200){
                        // pm
                        pm.put(delayMinute, s);
                        if (pm.size() > 1) {
                            pm.remove(pm.lastKey());
                        }
                    }
                }
                List<String> resultList = new ArrayList<String>();
                if (!am.isEmpty()) {
                    resultList.add(am.get(am.firstKey()));
                }

                if (!pm.isEmpty()) {
                    resultList.add(pm.get(pm.firstKey()));
                }

                return resultList;
            }
        });

        result.print();
        result.dstream().saveAsTextFiles("/Task2/Q32/","");

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
