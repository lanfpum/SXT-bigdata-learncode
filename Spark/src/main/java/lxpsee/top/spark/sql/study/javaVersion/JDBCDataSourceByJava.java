package lxpsee.top.spark.sql.study.javaVersion;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/26 15:38.
 */
public class JDBCDataSourceByJava {
    public static void main(String[] args) {
        final String MYSQL_URL = "jdbc:mysql://localhost:3306/lanp_test_db";
        final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

        final String STUDENT_INFOS = "student_infos";
        final String STUDENT_SCORES = "student_scores";
        final String GOOD_STUDENT_INFOS = "good_student_infos";

        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("JDBCDataSourceByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext);

        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123");

        Dataset<Row> studentInfosDF = sqlContext.read().jdbc(MYSQL_URL, STUDENT_INFOS, properties);
        Dataset<Row> studentScoresDF = sqlContext.read().jdbc(MYSQL_URL, STUDENT_SCORES, properties);

        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = studentInfosDF.javaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0),
                                Integer.valueOf(String.valueOf(row.get(1))));
                    }
                })
                .join(studentScoresDF.javaRDD().mapToPair(
                        new PairFunction<Row, String, Integer>() {
                            public Tuple2<String, Integer> call(Row row) throws Exception {
                                return new Tuple2<String, Integer>(row.getString(0),
                                        Integer.valueOf(String.valueOf(row.get(1))));
                            }
                        }));

        JavaRDD<Row> goodStudentRowRDD = joinRDD.map(
                new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
                    public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple2) throws Exception {
                        return RowFactory.create(tuple2._1, tuple2._2._1, tuple2._2._2);
                    }
                }).filter(
                new Function<Row, Boolean>() {
                    public Boolean call(Row row) throws Exception {
                        if (row.getInt(2) >= 80) return true;
                        return false;
                    }
                });

        List<StructField> structFields = new ArrayList<StructField>(3);
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> goodStudentsDF = sqlContext.createDataFrame(goodStudentRowRDD, structType);

       /* for (Row row : goodStudentsDF.collect()) {
            System.out.println(row);
        }*/

        //这种方式是在企业里很常用的，有可能是插入mysql、有可能是插入hbase，还有可能是插入redis缓存
        goodStudentsDF.javaRDD().foreach(
                new VoidFunction<Row>() {
                    public void call(Row row) throws Exception {
                        String sql = "INSERT INTO " + GOOD_STUDENT_INFOS + " VALUES('"
                                + row.getString(0) + "',"
                                + Integer.parseInt(String.valueOf(row.get(1))) + ","
                                + Integer.parseInt(String.valueOf(row.get(2))) + ")";

                        Class.forName(MYSQL_DRIVER);
                        Connection connection = null;
                        PreparedStatement ps = null;

                        try {
                            connection = DriverManager.getConnection(MYSQL_URL, "root", "123");
                            ps = connection.prepareStatement(sql);
                            ps.executeUpdate();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        } finally {
                            if (ps != null) ps.close();
                            if (connection != null) connection.close();
                        }

                    }
                });

        sparkContext.close();
    }
}
