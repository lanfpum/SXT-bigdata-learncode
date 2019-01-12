package lxpsee.top.initlearn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/9 19:09.
 *
 *  在vm opnion中配置 -DHADOOP_USER_NAME=lanp
 */
public class FirstHadoopDemo {
    public static void main(String[] args) throws Exception {
        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 直接配置访问集群的路径和访问集群的用户名称
        FileSystem fileSystem = FileSystem.get(configuration);

        // 2 把本地文件上传到文件系统中
        fileSystem.copyFromLocalFile(new Path("D:\\workDir\\bigdata\\shareJar\\a.txt"), new Path("/user/lanp/test/2.txt"));

        // 3 关闭资源
        fileSystem.close();
        System.out.println("over");
    }
}
