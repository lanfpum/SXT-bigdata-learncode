package lxpsee.top.spark.streaming.study.javaversion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
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

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/13 15:58.
 */
public class WCStreamingByJava {
    public static void main(String[] args) throws InterruptedException {
        /**
         * 创建SparkConf对象
         *  但是这里有一点不同，我们是要给它设置一个Master属性，但是我们测试的时候使用local模式
         *  local后面必须跟一个方括号，里面填写一个数字，数字代表了，我们用几个线程来执行我们的
         *  Spark Streaming程序
         */
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WCStreamingByJava").set("spark.testing.memory", "2147480000");

        /**
         * 创建JavaStreamingContext对象
         * 该对象，就类似于Spark Core中的JavaSparkContext，就类似于Spark SQL中的SQLContext
         * 该对象除了接收SparkConf对象对象之外
         * 还必须接收一个batch interval参数，就是说，每收集多长时间的数据，划分为一个batch，进行处理
         * 这里设置一秒
         */
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        /**
         * 首先，创建输入DStream，代表了一个从数据源（比如kafka、socket）来的持续不断的实时数据流
         * 调用JavaStreamingContext的socketTextStream()方法，可以创建一个数据源为Socket网络端口的
         * 数据流，JavaReceiverInputStream，代表了一个输入的DStream
         * socketTextStream()方法接收两个基本参数，第一个是监听哪个主机上的端口，第二个是监听哪个端口
         */
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(SparkSqlStudyConstants.NC_SERVER_IP, 9999);

        /**
         * 到这里为止，你可以理解为JavaReceiverInputDStream中的，每隔一秒，会有一个RDD，其中封装了
         * 这一秒发送过来的数据
         * RDD的元素类型为String，即一行一行的文本
         * 所以，这里JavaReceiverInputStream的泛型类型<String>，其实就代表了它底层的RDD的泛型类型
         *
         * 开始对接收到的数据，执行计算，使用Spark Core提供的算子，执行应用在DStream中即可
         * 在底层，实际上是会对DStream中的一个一个的RDD，执行我们应用在DStream上的算子
         * 产生的新RDD，会作为新DStream中的RDD
         */
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        /**
         * // 这个时候，每秒的数据，一行一行的文本，就会被拆分为多个单词，words DStream中的RDD的元素类型
         * 即为一个一个的单词
         *
         * 接着，开始进行flatMap、reduceByKey操作
         */
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
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.close();
    }
}
