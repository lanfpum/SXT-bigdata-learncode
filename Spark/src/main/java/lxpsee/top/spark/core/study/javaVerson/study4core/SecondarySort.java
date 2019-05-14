package lxpsee.top.spark.core.study.javaVerson.study4core;

import lxpsee.top.spark.core.study.constant.LPConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/27 17:10.
 * 二次排序
 * 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 * 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 */
public class SecondarySort {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf().setAppName("SecondarySort").setMaster("local"));
        JavaRDD<String> lines = sparkContext.textFile(LPConstants.SORT_TXT_LOCAL_FILE_PATH);
        JavaPairRDD<SecondarySortKey, String> wordRDD = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                SecondarySortKey key = new SecondarySortKey(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
                return new Tuple2<SecondarySortKey, String>(key, s);
            }
        });
        JavaPairRDD<SecondarySortKey, String> sortRDD = wordRDD.sortByKey();
        JavaRDD<String> mapRDD = sortRDD.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        mapRDD.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sparkContext.close();
    }
}
