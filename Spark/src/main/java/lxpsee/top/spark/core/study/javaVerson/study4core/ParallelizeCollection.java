package lxpsee.top.spark.core.study.javaVerson.study4core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/27 08:17.
 * * 并行化集合创建RDD
 * * 案例：累加1到10
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> parallelizeRDD = sc.parallelize(list);
        Integer sum = parallelizeRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        sc.close();
        System.out.println("1 -  10 的累加和：" + sum);
    }
}
