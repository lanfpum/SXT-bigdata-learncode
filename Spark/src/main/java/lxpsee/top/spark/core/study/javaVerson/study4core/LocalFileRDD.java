package lxpsee.top.spark.core.study.javaVerson.study4core;

import lxpsee.top.spark.core.study.constant.LPConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/27 08:48.
 */
public class LocalFileRDD {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LocalFileRDD")
                .setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile(LPConstants.SPARK_TXT_LOCAL_FILE_PATH);
        JavaRDD<Integer> length = lines.map(new Function<String, Integer>() {
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });
        Integer count = length.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        context.close();
        System.out.println("spark.txt total word is " + count);
    }
}
