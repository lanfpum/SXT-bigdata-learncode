package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/8 08:51.
 * <p>
 * row_number()开窗函数实战  求分组top n
 */
public class RowNumberWindowFunctionByJava {
    public static void main(String[] args) {
        final String SALES_TABLE = "lp_spark_study.sales";
        final String TOP3_SALES_TABLE = "lp_spark_study.top3_sales";

        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                        .setAppName("HiveDataSourceByJava")
                        .set("spark.testing.memory", "2147480000")
        );
        HiveContext hiveContext = new HiveContext(sparkContext.sc());

        // 创建销售额表，sales表
        hiveContext.sql("DROP TABLE IF EXISTS " + SALES_TABLE);
        hiveContext.sql("CREATE TABLE IF NOT EXISTS " + SALES_TABLE
                + " (product STRING, category STRING, revenue BIGINT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '"
                + SparkSqlStudyConstants.Hive_SALES_LOCAL_PATH
                + "' INTO TABLE " + SALES_TABLE);

        // 开始编写我们的统计逻辑，使用row_number()开窗函数
        // 先说明一下，row_number()开窗函数的作用
        // 其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号
        // 比如说，有一个分组date=20151001，里面有3条数据，1122，1121，1124,
        // 那么对这个分组的每一行使用row_number()开窗函数以后，三行，依次会获得一个组内的行号
        // 行号从1开始递增，比如1122 1，1121 2，1124 3
        Dataset<Row> top3DF = hiveContext.sql("SELECT product,category,revenue"
                + " FROM ("
                + "SELECT product,category,revenue,"
                + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
                + "FROM " + SALES_TABLE + ") tmp_sales "
                + " WHERE rank <= 3 ");
        hiveContext.sql("DROP TABLE IF EXISTS " + TOP3_SALES_TABLE);
        top3DF.write().mode(SaveMode.Overwrite).saveAsTable(TOP3_SALES_TABLE);

        sparkContext.close();
    }
}
