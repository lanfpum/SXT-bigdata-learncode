package lxpsee.top.spark.core.study.javaVerson.study4core;

import lxpsee.top.spark.core.study.constant.LPConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/27 18:01.
 * <p>
 * 取数字中最大的三个数字
 */
public class Top3 {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("Top3java"));
        JavaRDD<String> lines = sparkContext.textFile(LPConstants.TOP_TXT_LOCAL_FILE_PATH);
        JavaPairRDD<Integer, String> pairRDD = lines.mapToPair(new PairFunction<String, Integer, String>() {
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.parseInt(s), s);
            }
        });
        JavaPairRDD<Integer, String> sortByKey = pairRDD.sortByKey(false);
        JavaRDD<String> mapRDD = sortByKey.map(new Function<Tuple2<Integer, String>, String>() {
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._2;
            }
        });
        List<String> list = mapRDD.take(3);

        for (String s : list) {
            System.out.println(s);
        }
        sparkContext.close();
    }
}
