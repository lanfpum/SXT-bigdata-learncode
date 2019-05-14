package lxpsee.top.spark.core.study.javaVerson.study4core;

import lxpsee.top.spark.core.study.constant.LPConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/27 18:27.
 * 分组取top3
 * <p>
 * tuple2._2.iterator();
 * 分组后变换过程
 */
public class GroupTop3 {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("GroupTop3Java"));
        JavaRDD<String> lines = sparkContext.textFile(LPConstants.SCORE_TXT_LOCAL_FILE_PATH);
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> groupByKey = pairRDD.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> top3RDD = groupByKey.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                String className = tuple2._1;
                Iterator<Integer> scoreInterable = tuple2._2.iterator();
                Integer[] top3 = new Integer[3];

                while (scoreInterable.hasNext()) {
                    Integer score = scoreInterable.next();

                    for (int i = 0; i < 3; i++) {
                        if (top3[i] == null) {
                            top3[i] = score;
                            break;
                        } else if (top3[i] < score) {
                            for (int j = 2; j > i; j--) {
                                top3[j] = top3[j - 1];
                            }

                            top3[i] = score;
                            break;
                        }

                    }
                }

                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });
        top3RDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println(" class : " + tuple2._1);
                Iterator<Integer> iterator = tuple2._2.iterator();

                while (iterator.hasNext()) {
                    Integer next = iterator.next();
                    System.out.println("score : " + next);
                }

                System.out.println(" ------------------------- ");
            }
        });
        sparkContext.close();
    }
}
