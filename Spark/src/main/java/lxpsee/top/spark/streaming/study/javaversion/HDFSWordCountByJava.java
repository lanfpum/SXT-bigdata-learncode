package lxpsee.top.spark.streaming.study.javaversion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/13 16:18.
 */
public class HDFSWordCountByJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("HDFSWordCountByJava").set("spark.testing.memory", "2147480000");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
        JavaDStream<String> lines = jsc.textFileStream("D:\\workDir\\otherFile\\temp\\2019-4\\test");
//        JavaDStream<String> lines = streamingContext.textFileStream(SparkSqlStudyConstants.WC_HDFS_FILE_PATH);
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String line) throws Exception {
                        return Arrays.asList(line.split(" ")).iterator();
                    }
                });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
