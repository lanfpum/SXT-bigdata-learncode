package lxpsee.top.spark.sql.study.sqlConstant;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/5/10 12:42.
 */
public class GenKeyWordData {
    public static void main(String[] args) throws IOException {
        String fileName = "D:/workDir/otherFile/temp/2019-5/keyword.txt";
        int times = 500;

        genLogs(fileName, times);
    }

    private static void genLogs(String fileName, int times) throws IOException {
        FileWriter fileWriter = new FileWriter(fileName, false);

        List<String> dates = Arrays.asList("2019-03-08", "2019-04-11", "2019-05-20");
        List<String> keywords = Arrays.asList("hadoop", "hive", "scala", "sql", "redis", "mongodb");
        List<String> users = Arrays.asList("jim", "kobe", "TOM", "HARRY", "ALICE");
        List<String> cities = Arrays.asList("bj", "", "sh", "sz");
        List<String> platforms = Arrays.asList("android", "ios");
        List<String> versions = Arrays.asList("1.2", "1.3");
        Random random = new Random();

        for (int i = 0; i < times; i++) {
            String keyword;

            if (i % 2 == 0 || i % 3 == 0 || i % 5 == 0) {
                keyword = Arrays.asList("hadoop", "hive", "scala").get(random.nextInt(3));
            } else {
                keyword = keywords.get(random.nextInt(keywords.size()));
            }

            String date = dates.get(random.nextInt(dates.size()));
            String user = users.get(random.nextInt(users.size()));

            String city = cities.get(random.nextInt(cities.size()));
            String platform = platforms.get(random.nextInt(platforms.size()));
            String version = versions.get(random.nextInt(versions.size()));

            String log = date + "\t" + keyword + "\t" + user + "\t" + city + "\t" + platform + "\t" + version + "\r\n";

            fileWriter.write(log);
            fileWriter.flush();
        }

        fileWriter.close();
    }
}
