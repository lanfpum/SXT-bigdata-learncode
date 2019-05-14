package lxpsee.top.spark.sql.study.javaVersion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/8 09:55.
 */
public class UDFByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("UDFByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext.sc());
        List<String> names = new ArrayList<String>(4);
        names.add("leo");
        names.add("Marry");
        names.add("Jack");
        names.add("g");

        JavaRDD<String> namesRDD = sparkContext.parallelize(names);
        JavaRDD<Row> nameRowRDD = namesRDD.map(new Function<String, Row>() {
            public Row call(String v1) throws Exception {
                return RowFactory.create(v1);
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> nameDF = sqlContext.createDataFrame(nameRowRDD, structType);
        nameDF.registerTempTable("names");

        // 用户自定义函数应该设置返回值类型
        sqlContext.udf().register("strLen",
                new UDF1<String, Integer>() {
                    public Integer call(String s) throws Exception {
                        return s.length();
                    }
                },
                DataTypes.IntegerType);

        sqlContext.sql("SELECT name,strLen(name) FROM names").foreach(
                new ForeachFunction<Row>() {
                    public void call(Row row) throws Exception {
                        System.out.println(row);
                    }
                });

        sparkContext.close();
    }
}
