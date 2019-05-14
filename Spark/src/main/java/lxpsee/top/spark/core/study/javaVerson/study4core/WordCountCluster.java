package lxpsee.top.spark.core.study.javaVerson.study4core;

import lxpsee.top.spark.core.study.constant.LPConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/25 17:44.
 * <p>
 * 将java开发的wordcount程序部署到spark集群上运行
 */
public class WordCountCluster {
    public static void main(String[] args) {

        /**
         * 如果要在spark集群上运行，需要修改的，只有两个地方
         * 		第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
         * 		第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件
         *
         * 		实际执行步骤：
         * 		1、将spark.txt文件上传到hdfs上去
         * 		2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
         * 		3、将打包后的spark工程jar包，上传到机器上执行
         * 		4、编写spark-submit脚本
         * 		5、执行spark-submit脚本，提交spark应用到集群执行
         */

        SparkConf sparkConf = new SparkConf().setAppName("WordCountCluster").set("spark.testing.memory", "2147480000");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile(LPConstants.SPARK_TXT_HDFS_FILE_PATH);
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        JavaPairRDD<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> wordcounts = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        wordcounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2._1 + " appeared " + tuple2._2 + " times.");
            }
        });

        sparkContext.close();
    }
}
