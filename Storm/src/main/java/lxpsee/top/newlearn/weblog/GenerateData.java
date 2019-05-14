package lxpsee.top.newlearn.weblog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 22:05.
 * <p>
 * 网站访问日志生成工具类
 * 网站域名，会话id，访问网站时间
 */
public class GenerateData {
    public static void main(String[] args) {
        File logFile = new File("D:\\workDir\\otherFile\\temp\\2019-3\\weblog.txt");
        String[] hosts = {"lxpsee.top"};
        String[] session_id = {"ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34",
                "BBYH61456FGHHJ7JL89RG5VV9UYU7", "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678"};
        String[] time = {"2019-3-13 08:40:50", "2019-3-13 08:40:51", "2019-3-13 08:40:52", "2019-3-13 08:40:53",
                "2019-3-13 09:40:49", "2019-3-13 10:40:49", "2019-3-13 11:40:49", "2019-3-13 12:40:49"};

        Random random = new Random();
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < 50; i++) {
            builder.append(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)] + "\n");
        }

        if (!logFile.exists()) {
            try {
                logFile.createNewFile();
            } catch (IOException e) {
                System.out.println("create fail");
            }
        }

        FileOutputStream fs;
        try {
            fs = new FileOutputStream(logFile);
            fs.write(builder.toString().getBytes());
            fs.close();
            System.out.println("generate data over");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
