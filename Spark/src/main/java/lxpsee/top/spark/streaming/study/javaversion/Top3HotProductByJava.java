package lxpsee.top.spark.streaming.study.javaversion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/16 08:54.
 * <p>
 * 与Spark SQL整合使用，top3热门商品实时统计
 */
public class Top3HotProductByJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Top3HotProductByJava");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // user priduct category 日志格式，转换成category_product,1,进行窗口长度60s，间隔10s统计
        JavaReceiverInputDStream<String> userProductCategoryDS = jsc.socketTextStream(
                SparkSqlStudyConstants.NC_SERVER_IP, 9999);
        JavaPairDStream<String, Integer> categoryProductsDS = userProductCategoryDS.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String productClickLog) throws Exception {
                        String[] logArr = productClickLog.split(" ");
                        return new Tuple2<String, Integer>(logArr[2] + "_" + logArr[1], 1);
                    }
                }
        );
        JavaPairDStream<String, Integer> productCategoryCountDS = categoryProductsDS.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                },
                Durations.seconds(60),
                Durations.seconds(10)
        );
        // 对统计结果使用spark sql进行变换，直接使用foreachRDD计算。对每一个rdd转换成rowrdd，通过元数据转换
        // 注册临时表，使用开窗函数获取点击次数排名前3的热门商品的rdd，本次直接输出，实际中存入数据库或者redis缓存
        // 不能用hivecontext 无法执行
        productCategoryCountDS.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Integer>>() {
                    @Override
                    public void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {
                        JavaRDD<Row> categoryProductCountRowRDD = categoryProductCountsRDD.map(
                                new Function<Tuple2<String, Integer>, Row>() {
                                    @Override
                                    public Row call(Tuple2<String, Integer> tuple2) throws Exception {
                                        String[] categoryProductArr = tuple2._1.split("_");
                                        return RowFactory.create(categoryProductArr[0], categoryProductArr[1], tuple2._2);
                                    }
                                }
                        );


                        List<StructField> structFields = new ArrayList<StructField>(3);
                        structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                        structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                        structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
                        StructType structType = DataTypes.createStructType(structFields);

                        SQLContext sqlContext = new SQLContext(categoryProductCountsRDD.context());

                        Dataset<Row> categoryProductCountDF = sqlContext.createDataFrame(categoryProductCountRowRDD, structType);

                        categoryProductCountDF.registerTempTable("product_click_log");

                        Dataset<Row> top3ProductDF = sqlContext.sql(
                                "SELECT category,product,click_count FROM ("
                                        + "SELECT category,product,click_count,"
                                        + "row_number() OVER (PARTITION BY category ORDER BY click_count DESC) rank "
                                        + "FROM product_click_log"
                                        + ") tmp WHERE rank <= 3");

                        /**
                         * 案例说，应该将数据保存到redis缓存、或者是mysql db中
                         * 	然后，应该配合一个J2EE系统，进行数据的展示和查询、图形报表
                         */
                        top3ProductDF.show();
                    }
                }
        );

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
