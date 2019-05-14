package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/10 11:11.
 * <p>
 * 每日top3热点搜索词统计案例
 */
public class DailyTop3KeywordByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf().setAppName("DailyTop3KeywordByJava").set("spark.testing.memory", "2147480000")
//                new SparkConf().setAppName("DailyTop3KeywordByJava").setMaster("local")
        );
        HiveContext hiveContext = new HiveContext(sparkContext.sc());

        /**
         * 伪造出一份数据，查询条件
         * 备注：实际上，在实际的企业项目开发中，很可能，这个查询条件，是通过J2EE平台插入到某个MySQL表中的
         * 然后，这里呢，实际上，通常是会用Spring框架和ORM框架（MyBatis）的，去提取MySQL表中的查询条件
         *
         * 根据我们实现思路中的分析，这里最合适的方式，是将该查询参数Map封装为一个Broadcast广播变量
         * 这样可以进行优化，每个Worker节点，就拷贝一份数据即可
         */
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("bj", "sz", "sh"));
        queryParamMap.put("platform", Arrays.asList("android", "ios"));
        queryParamMap.put("version", Arrays.asList("1.2", "1.3"));
        final Broadcast<Map<String, List<String>>> queryParamMapBroadcast = sparkContext.broadcast(queryParamMap);

        JavaRDD<String> keywordRDD = sparkContext.textFile(SparkSqlStudyConstants.KEY_WORD_HDFS_FILE_PATH);
//        final JavaRDD<String> keywordRDD = sparkContext.textFile(SparkSqlStudyConstants.KEY_WORD_LOCAL_FILE_PATH);

        /**
         * 1.过滤出来的原始日志，映射为(日期_搜索词, 用户)的格式
         * 2.进行分组，获取每天每个搜索词，有哪些用户搜索了（没有去重）
         * 3.对每天每个搜索词的搜索用户，执行去重操作，获得其uv
         * 4.将每天每个搜索词的uv数据，转换成DataFrame
         * 5.使用Spark SQL的开窗函数，统计每天搜索uv排名前3的热点搜索词
         * 6.DataFrame转换为RDD，然后映射((日期,搜索词_uv)，计算出每天的top3搜索词的搜索uv总数
         * 7.按照每天的总搜索uv进行倒序排序
         * 8.再次进行映射，将排序后的数据，映射回原始的格式，Iterable<Row>
         */
        JavaRDD<String> filterLogRDD = keywordRDD.filter(
                new Function<String, Boolean>() {
                    public Boolean call(String log) throws Exception {
                        String[] logArr = log.split("\t");

                        if (logArr.length < 6) return false;

                        String city = logArr[3];
                        String platform = logArr[4];
                        String version = logArr[5];

                        Map<String, List<String>> queryParamMap = queryParamMapBroadcast.value();
                        List<String> cities = queryParamMap.get("city");
                        List<String> platforms = queryParamMap.get("platform");
                        List<String> versions = queryParamMap.get("version");

                        if (cities.size() > 0 && !cities.contains(city)) return false;

                        if (platforms.size() > 0 && !platforms.contains(platform)) return false;

                        if (versions.size() > 0 && !versions.contains(version)) return false;

                        return true;
                    }
                });
        JavaPairRDD<String, String> dateKeywordUserRDD = filterLogRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String log) throws Exception {
                        String[] logArr = log.split("\t");
                        String key = logArr[0] + "_" + logArr[1];
                        return new Tuple2<String, String>(key, logArr[2]);
                    }
                });
        JavaPairRDD<String, Iterable<String>> dateKeywordUsersRDD = dateKeywordUserRDD.groupByKey();
        JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
                    public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                        Iterator<String> userIterator = tuple2._2.iterator();
                        List<String> distinctUsers = new ArrayList<String>();

                        while (userIterator.hasNext()) {
                            String user = userIterator.next();

                            if (!distinctUsers.contains(user)) distinctUsers.add(user);
                        }

                        return new Tuple2<String, Long>(tuple2._1, (long) distinctUsers.size());
                    }
                });
        JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(
                new Function<Tuple2<String, Long>, Row>() {
                    public Row call(Tuple2<String, Long> tuple2) throws Exception {
                        String date = tuple2._1.split("_")[0];
                        String keyWord = tuple2._1.split("_")[1];
                        return RowFactory.create(date, keyWord, tuple2._2);
                    }
                });
        StructType structType = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
        ));
        Dataset<Row> dateKeywordUvDF = hiveContext.createDataFrame(dateKeywordUvRowRDD, structType);
        dateKeywordUvDF.registerTempTable("daily_keyword_uv");
        Dataset<Row> dailyTop3KeywordDF = hiveContext.sql("SELECT date,keyword,uv "
                + "FROM ("
                + "SELECT "
                + "date,keyword,uv,"
                + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
                + "FROM daily_keyword_uv "
                + ") tmp "
                + "WHERE rank <= 3");
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordDF.javaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String date = row.getString(0);
                        String keyword = row.getString(1);
                        Long uv = row.getLong(2);
                        return new Tuple2<String, String>(date, keyword + "_" + uv);
                    }
                });
        JavaPairRDD<Long, String> uvDateKeyWordRDD = top3DateKeywordUvRDD.groupByKey().mapToPair(
                new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                        String date = tuple2._1;
                        String dateKeyworduv = date;
                        Long totalUV = 0l;
                        Iterator<String> keyworduvIT = tuple2._2.iterator();

                        while (keyworduvIT.hasNext()) {
                            String keyworduv = keyworduvIT.next();
                            totalUV += Long.valueOf(keyworduv.split("_")[1]);
                            dateKeyworduv += "," + keyworduv;
                        }

                        return new Tuple2<Long, String>(totalUV, dateKeyworduv);
                    }
                });
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeyWordRDD.sortByKey(false);
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(
                new FlatMapFunction<Tuple2<Long, String>, Row>() {
                    public Iterator<Row> call(Tuple2<Long, String> tuple2) throws Exception {
                        String[] arr = tuple2._2.split(",");
                        String date = arr[0];
                        List<Row> rows = new ArrayList<Row>(3);
                        rows.add(RowFactory.create(date, arr[1].split("_")[0], Long.valueOf(arr[1].split("_")[1])));
                        rows.add(RowFactory.create(date, arr[2].split("_")[0], Long.valueOf(arr[2].split("_")[1])));
                        rows.add(RowFactory.create(date, arr[3].split("_")[0], Long.valueOf(arr[3].split("_")[1])));
                        return rows.iterator();
                    }
                });
        Dataset<Row> finalDF = hiveContext.createDataFrame(sortedRowRDD, structType);
        finalDF.show();
        finalDF.write().mode(SaveMode.Overwrite).saveAsTable("daily_top3_keyword_uv");

        sparkContext.close();
    }

}
