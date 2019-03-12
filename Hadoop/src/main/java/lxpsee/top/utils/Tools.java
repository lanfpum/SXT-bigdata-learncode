package lxpsee.top.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/13 08:48.
 */
public class Tools {
    private static Configuration configuration = new Configuration();

    /**
     * 向HDFS中上传文件
     *
     * @param src 上传文件路径
     * @param dst 上传到hdfs中的路径
     */
    public static void upLoadFile2HDFS(String src, String dst) {
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);
            long startTime = System.currentTimeMillis();
            fileSystem.copyFromLocalFile(false, srcPath, dstPath);
            System.out.println("耗时" + (System.currentTimeMillis() - startTime) + "ms");
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 输出HDFS中的文件到控制台
     *
     * @param src hdfs中的路径
     */
    public static void showFileFromHDFS2Console(String src) {
        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream inputStream = fileSystem.open(new Path(src));
            IOUtils.copyBytes(inputStream, System.out, 1024, false);
            IOUtils.closeStream(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除指定文件或目录
     *
     * @param directory "/user/hive/warehouse/syllabus.db/track_log/date=20150828"
     * @param exist     "hour=18"
     */
    public static void deleteFileInHDFS(String directory, String exist) {
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(directory), configuration);
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(directory));

            for (FileStatus fileStatus : fileStatuses) {

                if (fileStatus.getPath().getName().startsWith(exist)) {
                    fileSystem.delete(fileStatus.getPath(), true);
                }
            }
        } catch (IOException e) {
            System.out.println("there is no bug!");
        }
    }

    /**
     * 打印指定目录下的所有文件
     *
     * @param directory
     * @throws IOException
     */
    public static void getDirectoryFromHDFS(String directory) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(directory), configuration);
        FileStatus[] fileList = fileSystem.listStatus(new Path(directory));
        System.out.println("_________________***********************____________________");
        for (int i = 0; i < fileList.length; i++) {
            FileStatus fileStatus = fileList[i];
            System.out.println("Name:" + fileStatus.getPath().getName());
            System.out.println("Size:" + fileStatus.getLen());
        }
        System.out.println("_________________***********************____________________");

        fileSystem.close();
    }
}
