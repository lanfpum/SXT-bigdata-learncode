package lxpsee.top.sparkBookLearn.javaVersion;

import lxpsee.top.sparkBookLearn.javaVersion.javaBean.AvgCount;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/15 08:28.
 */
public class SquareDemoJava {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("SquareDemoJava"));
        JavaRDD<Integer> input = sc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaRDD<Integer> result = input.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                return v1 * v1;
            }
        });
        System.out.println(StringUtils.join(result.collect(), ","));

        AvgCount initial = new AvgCount(0, 0);
        AvgCount resAvg = input.aggregate(initial,
                new Function2<AvgCount, Integer, AvgCount>() {
                    public AvgCount call(AvgCount v1, Integer v2) throws Exception {
                        v1.setTotal(v1.getTotal() + v2);
                        v1.setNum(v1.getNum() + 1);
                        return v1;
                    }
                },
                new Function2<AvgCount, AvgCount, AvgCount>() {
                    public AvgCount call(AvgCount v1, AvgCount v2) throws Exception {
                        v1.setTotal(v1.getTotal() + v2.getTotal());
                        v1.setNum(v1.getNum() + v2.getNum());
                        return v1;
                    }
                });
        System.out.println(resAvg.avg());
    }
}


