package lxpsee.top.spark.streaming.study.javaversion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/15 17:40.
 * 基于transform的实时广告计费日志黑名单过滤
 * * 这里案例，完全脱胎于真实的广告业务的大数据系统，业务是真实的，实用
 */
public class TransformBlacklistByJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]")
                .setAppName("TransformBlacklistByJava")
                /*.set("spark.testing.memory", "2147480000")*/;
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        List<Tuple2<String, Boolean>> blackList = new ArrayList<Tuple2<String, Boolean>>();
        blackList.add(new Tuple2<String, Boolean>("tom", true));

        // 注意应该是 parallelizePairs 才能join
        final JavaPairRDD<String, Boolean> blackListRDD = jsc.sparkContext().parallelizePairs(blackList);

        final JavaReceiverInputDStream<String> adsClickLogDStream = jsc.socketTextStream("ip201", 9999);

        // date username 的输入变成 (username, date username) 方便下面的过滤
        JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String adsClickLog) throws Exception {
                        return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
                    }
                }
        );

        // 先进行join，然后对其进行过滤，再将结果转换成date username 的rdd 返回
        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(
                new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
                    public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = userAdsClickLogRDD.leftOuterJoin(blackListRDD);
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinRDD.filter(
                                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {

                                        if (tuple2._2._2.isPresent() && tuple2._2._2.get()) return false;

                                        return true;
                                    }
                                }
                        );
                        JavaRDD<String> validAdsClickLogRDD = filterRDD.map(
                                new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple2) throws Exception {
                                        return tuple2._2._1;
                                    }
                                }
                        );
                        return validAdsClickLogRDD;
                    }
                }
        );

        validAdsClickLogDStream.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
