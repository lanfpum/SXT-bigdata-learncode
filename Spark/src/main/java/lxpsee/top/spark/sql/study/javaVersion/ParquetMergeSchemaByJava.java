package lxpsee.top.spark.sql.study.javaVersion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/24 11:04.
 */
public class ParquetMergeSchemaByJava {
    public static void main(String[] args) {
        final String FILE_DIR = "hdfs://lanpengcluster/user/lanp/testJar/spark-sql-study/testfile/student";

        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf()
                        .setAppName("ParquetMergeSchemaByJava")
                        .setMaster("spark://ip201:7077")
                        .set("spark.testing.memory", "2147480000"));

        SQLContext sqlContext = new SQLContext(sparkContext);

        List<Tuple2<String, Integer>> nameAndAge = new ArrayList<Tuple2<String, Integer>>(3);
        nameAndAge.add(new Tuple2<String, Integer>("jim", 18));
        nameAndAge.add(new Tuple2<String, Integer>("tom", 20));
        nameAndAge.add(new Tuple2<String, Integer>("kobe", 36));
        JavaRDD<Row> rowJavaRDD = sparkContext.parallelize(nameAndAge)
                .map(new Function<Tuple2<String, Integer>, Row>() {
                    public Row call(Tuple2<String, Integer> tuple2) throws Exception {
                        return RowFactory.create(
                                tuple2._1.trim(), Integer.valueOf(tuple2._2.toString().trim()));
                    }
                });

        List<StructField> nameAndAgeStructFields = new ArrayList<StructField>(2);
        nameAndAgeStructFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        nameAndAgeStructFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType nameAndAgeStructType = DataTypes.createStructType(nameAndAgeStructFields);

        Dataset<Row> nameAndAgeDF = sqlContext.createDataFrame(rowJavaRDD, nameAndAgeStructType);
        nameAndAgeDF.write().mode(SaveMode.Append).parquet(FILE_DIR);

        List<Tuple2<String, String>> nameAndGrade = new ArrayList<Tuple2<String, String>>(3);
        nameAndGrade.add(new Tuple2<String, String>("curry", "A"));
        nameAndGrade.add(new Tuple2<String, String>("iven", "A"));
        nameAndGrade.add(new Tuple2<String, String>("hade", "B"));
        JavaRDD<Row> nameAndGradeRDD = sparkContext.parallelize(nameAndGrade)
                .map(new Function<Tuple2<String, String>, Row>() {
                    public Row call(Tuple2<String, String> tuple2) throws Exception {
                        return RowFactory.create(tuple2._1.toString().trim(), tuple2._2.toString().trim());
                    }
                });

        List<StructField> nameAndGradeFields = new ArrayList<StructField>(2);
        nameAndGradeFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        nameAndGradeFields.add(DataTypes.createStructField("grade", DataTypes.StringType, true));
        StructType nameAndGradeStruct = DataTypes.createStructType(nameAndGradeFields);

        Dataset<Row> gradeDF = sqlContext.createDataFrame(nameAndGradeRDD, nameAndGradeStruct);

        gradeDF.write().mode(SaveMode.Append).parquet(FILE_DIR);

        Dataset<Row> readDF = sqlContext.read().option("mergeSchema", "true").parquet(FILE_DIR);
        readDF.printSchema();
        readDF.show();
    }
}
