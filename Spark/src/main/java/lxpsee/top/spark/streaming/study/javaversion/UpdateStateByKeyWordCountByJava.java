package lxpsee.top.spark.streaming.study.javaversion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/15 08:42.
 * <p>
 * 基于updateStateByKey算子实现缓存机制的实时wordcount程序
 */
public class UpdateStateByKeyWordCountByJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]")
                .setAppName("UpdateStateByKeyWordCountByJava")
                .set("spark.testing.memory", "2147480000");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        /**
         * 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
         * 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
         * 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在
         * 内存数据丢失的时候，可以从checkpoint中恢复数据
         *
         * 开启checkpoint机制，很简单，只要调用jssc的checkpoint()方法，设置一个hdfs目录即可
         */
        jsc.checkpoint(SparkSqlStudyConstants.CHECK_POINT_HDFS_PATH);

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("ip201", 9999);
        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<String, String>() {
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split(" ")).iterator();
                    }
                }
        );
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }
        );
        /**
         * 这里的Optional，相当于Scala中的样例类，就是Option，可以这么理解
         *  它代表了一个值的存在状态，可能存在，也可能不存在
         * 这里两个参数
         * 	实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
         *  第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
         * 	比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
         * 	第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
         *
         * 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
         * 	如果存在，说明这个key之前已经统计过全局的次数了
         *
         * 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计次数
         */
        JavaPairDStream<String, Integer> wordcount = pairs.updateStateByKey(
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
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

        wordcount.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
