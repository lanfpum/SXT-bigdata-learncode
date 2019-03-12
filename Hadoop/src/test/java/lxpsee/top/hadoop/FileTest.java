package lxpsee.top.hadoop;

import lxpsee.top.utils.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 09:16.
 */
public class FileTest {

    @Test
    public void testDeletePathFile() {
        File file = new File("/D:\\workDir\\otherFile\\temp\\2019-1\\order\\output1");

        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testSubStr() {
        String path = "file:///D:\\workDir\\otherFile\\temp\\2019-1\\order\\output1";
        System.out.println(path.substring(8));
    }


    @Test
    public void testOutput() throws IOException {
        String path = "file:/D:/workDir/otherFile/temp/2019-1/outputformat/lxpsee.log";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path));
        fsDataOutputStream.write("hello".getBytes());
        fsDataOutputStream.close();
    }


    @Test
    public void testTools() {
        Tools.deleteFileInHDFS("/user/hive/warehouse/lp_db_hive_demo.db/like_track_log/date=20150828", "hour=18");
    }
}
