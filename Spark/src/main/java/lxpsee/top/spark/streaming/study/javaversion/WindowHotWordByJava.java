package lxpsee.top.spark.streaming.study.javaversion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/15 18:37.
 * <p>
 * 热点搜索词滑动统计，每隔10秒钟，统计最近60秒钟的搜索词的搜索频次，并打印出排名最靠前的3个搜索词以及出现次数
 */
public class WindowHotWordByJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWordByJava");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 数据格式是user words
        JavaReceiverInputDStream<String> userWordsDS = jsc.socketTextStream("ip201", 9999);
        JavaPairDStream<String, Integer> wordPairDS = userWordsDS.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String userWord) throws Exception {
                        return new Tuple2<String, Integer>(userWord.split(" ")[1], 1);
                    }
                }
        );
        //第二个参数，是窗口长度，这里是60秒;第三个参数，是滑动间隔，这里是10秒
        JavaPairDStream<String, Integer> searchWordCountsDStream = wordPairDS.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                },
                Durations.seconds(60),
                Durations.seconds(10)
        );
        // 对窗口中的数据进行计算，先反转，降序排序，再反转，取前三，输出控制台，将RDD原样返回  注意用的方法是 transformToPair
        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {
                        JavaPairRDD<Integer, String> countSearchWordsRDD = searchWordCountsRDD.mapToPair(
                                new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                                        return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
                                    }
                                }
                        );
                        JavaPairRDD<Integer, String> sortedCountSearchWordsRDD = countSearchWordsRDD.sortByKey(false);
                        JavaPairRDD<String, Integer> sortedSearchWordCountsRDD = sortedCountSearchWordsRDD.mapToPair(
                                new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                                        return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
                                    }
                                }
                        );
                        List<Tuple2<String, Integer>> hotSearchWordCounts = sortedSearchWordCountsRDD.take(3);

                        for (Tuple2<String, Integer> tuple2 : hotSearchWordCounts) {
                            System.out.println(tuple2._1 + " : " + tuple2._2);
                        }

                        return searchWordCountsRDD;
                    }
                }
        );

        finalDStream.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
