package lxpsee.top.spark.sql.study.javaVersion;

import lxpsee.top.spark.sql.study.model.Student;
import lxpsee.top.spark.sql.study.sqlConstant.SparkSqlStudyConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/23 15:02.
 * <p>
 * 使用反射的方式将RDD转换为DataFrame
 * <p>
 * 使用反射方式，将RDD转换为DataFrame
 * 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
 * 因为Student.class本身就是反射的一个应用
 * 然后底层还得通过对Student Class进行反射，来获取其中的field
 * 这里要求，JavaBean必须实现Serializable接口，是可序列化的
 * <p>
 * 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
 * 针对students临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teenageer
 * 将查询出来的DataFrame，再次转换为RDD
 * 将RDD中的数据，进行映射，映射为Student
 * <p>
 * row中的数据的顺序，可能是跟我们期望的是不一样的！
 */
public class RDD2DataFrameReflectionByJava {
    public static void main(String[] args) {
        JavaSparkContext sparkContext = new JavaSparkContext(
                new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflectionByJava"));
        SQLContext sqlContext = new SQLContext(sparkContext);

        JavaRDD<String> lines = sparkContext.textFile(SparkSqlStudyConstants.STUDENTS_TXT_LOCAL_FILE_PATH);
        JavaRDD<Student> studentsRDD = lines.map(new Function<String, Student>() {
            public Student call(String line) throws Exception {
                String[] arr = line.split(",");
                return new Student(
                        Integer.parseInt(arr[0].trim()),
                        arr[1], Integer.parseInt(arr[2].trim()));
            }
        });
        Dataset<Row> studentFrame = sqlContext.createDataFrame(studentsRDD, Student.class);
        studentFrame.registerTempTable("students");
        Dataset<Row> studentData = sqlContext.sql("select * from students where age < 18");
        JavaRDD<Row> teenagerRDD = studentData.javaRDD();
        JavaRDD<Student> teenagerStudentRDD = teenagerRDD.map(new Function<Row, Student>() {
            public Student call(Row row) throws Exception {
                return new Student(row.getInt(0), row.getString(2), row.getInt(1));
            }
        });
        List<Student> studentList = teenagerStudentRDD.collect();
        for (Student student : studentList) {
            System.out.println(student);
        }
    }
}
