package lxpsee.top.spark.sql.study.sqlConstant;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/4/22 08:59.
 */
public class SparkSqlStudyConstants {
    private static final String LOCAL_4_DISK_PATH = "D://workDir//otherFile//temp//2019-4//";
    private static final String LOCAL_5_DISK_PATH = "D://workDir//otherFile//temp//2019-5//";
    private static final String HDFS_PREFIX = "hdfs://lanpengcluster/user/lanp/testJar/spark-sql-study/testfile/";

    public static final String STUDENTS_JSON_LOCAL_FILE_PATH = LOCAL_4_DISK_PATH + "students.json";
    public static final String STUDENTS_JSON_HDFS_FILE_PATH = HDFS_PREFIX + "students.json";

    public static final String STUDENTS_SCORE_JSON_HDFS_FILE_PATH = HDFS_PREFIX + "studentsAndScore.json";
    public static final String STUDENTS_SCORE_JSON_LOCAL_FILE_PATH = LOCAL_4_DISK_PATH + "studentsAndScore.json";

    public static final String STUDENTS_TXT_LOCAL_FILE_PATH = LOCAL_4_DISK_PATH + "students.txt";
    public static final String STUDENTS_TXT_HDFS_FILE_PATH = HDFS_PREFIX + "students.txt";

    public static final String USER_PARQUET_LOCAL_FILE_PATH = LOCAL_4_DISK_PATH + "users.parquet";
    public static final String PEOPLE_JSON_LOCAL_FILE_PATH = LOCAL_4_DISK_PATH + "people.json";

    public static final String OUT_PUT_DIR_LOCAL = "D://workDir//otherFile//temp//2019-4//output";
    public static final String HDFS_OUT_PUT_DIR = "hdfs://lanpengcluster/user/lanp/test";

    private static final String LINUX_LOCAL_DATA_FILE_PREFIX = "/home/lanp/lanpBigData/spark-study/studyTestFile/";

    public static final String Hive_STUDENT_INFO_LOCAL_PATH = LINUX_LOCAL_DATA_FILE_PREFIX + "student_infos.txt";
    public static final String Hive_STUDENT_SCORE_LOCAL_PATH = LINUX_LOCAL_DATA_FILE_PREFIX + "student_scores.txt";

    public static final String Hive_SALES_LOCAL_PATH = LINUX_LOCAL_DATA_FILE_PREFIX + "sales.txt";

    public static final String KEY_WORD_HDFS_FILE_PATH = HDFS_PREFIX + "keyword.txt";
    public static final String KEY_WORD_LOCAL_FILE_PATH = LOCAL_5_DISK_PATH + "keyword.txt";

    public static final String WC_LOCAL_FILE_PATH = LOCAL_4_DISK_PATH + "hello.txt";
    public static final String WC_HDFS_FILE_PATH = HDFS_PREFIX + "hello.txt";

    public static final String CHECK_POINT_HDFS_PATH = HDFS_PREFIX + "check-point/";





}
