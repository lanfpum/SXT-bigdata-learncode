package lxpsee.top.spark.streaming.study.javaversion.output;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/15 20:00.
 * <p>
 * 基于持久化机制的实时wordcount程序
 */
public class PersistWordCountByJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]")
                .setAppName("PersistWordCountByJava");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(90));

        jsc.checkpoint(SparkSqlStudyConstants.CHECK_POINT_HDFS_PATH);

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("ip201", 9999);
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                    @Override
                    public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                        Integer totalCount = 0;

                        if (state.isPresent()) totalCount = state.get();

                        for (Integer value : values) {
                            totalCount += value;
                        }

                        return Optional.of(totalCount);
                    }
                }
        );

        //每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化
        wordCounts.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Integer>>() {
                    @Override
                    public void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                        wordCountsRDD.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                                        Connection connection = ConnectionPool.getConnection();
                                        Statement statement = connection.createStatement();
                                        Tuple2 wordcount = null;

                                        while (wordCounts.hasNext()) {
                                            wordcount = wordCounts.next();
                                            String sql = "INSERT INTO wordcount(word,count) VALUES('"
                                                    + wordcount._1 + "',"
                                                    + wordcount._2 + ")";
                                            statement.execute(sql);
                                        }

                                        ConnectionPool.returnCOnmnection(connection);
                                    }
                                }
                        );
                    }
                }
        );

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
