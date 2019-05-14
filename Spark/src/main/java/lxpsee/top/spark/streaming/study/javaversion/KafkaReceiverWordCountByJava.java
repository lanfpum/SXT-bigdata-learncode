package lxpsee.top.spark.streaming.study.javaversion;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
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

import java.util.*;

//import org.apache.spark.streaming.kafka.KafkaUtils;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/14 10:29.
 * <p>
 * 基于Kafka receiver方式的实时wordcount程序    实操不行！！！！
 */
public class KafkaReceiverWordCountByJava {
    public static void main(String[] args) throws InterruptedException {
        final String zkQuorum = "192.168.217.201:2181,192.168.217.202:2181,192.168.217.203:2181";
        final String brokers = "192.168.217.202:9092,192.168.217.203:9092,192.168.217.204:9092";
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCountByJava")
                .set("spark.testing.memory", "2147480000");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流,指定每个topic使用多少个线程进行并发,The group id for this consumer
        Map<String, Object> topicThreadMap = new HashMap<String, Object>();
//        topicThreadMap.put("wordcount", 1);
        topicThreadMap.put("bootstrap.servers", brokers);
        topicThreadMap.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        topicThreadMap.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        topicThreadMap.put("group.id", "g1");
//        topicThreadMap.put("metadata.broker.list", zkQuorum);

        Set<String> topicSet = new HashSet<String>();
        topicSet.add("wordcount");


        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet, topicThreadMap));

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<ConsumerRecord<Object, Object>, String>() {
                    public Iterator<String> call(ConsumerRecord<Object, Object> t) throws Exception {
                        return (Iterator<String>) Arrays.asList(t.value().toString().split(" "));
                    }
                }
        );

        JavaPairDStream<String, Integer> paris = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        JavaPairDStream<String, Integer> wordcounts = paris.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        wordcounts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
