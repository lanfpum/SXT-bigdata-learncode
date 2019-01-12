package lxpsee.top.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/9 19:01.
 * <p>
 * -DHADOOP_USER_NAME=lanp
 */
public class HdfsClientDemo1Test {

    private static FileSystem fileSystem;

    static {

        try {
            Configuration configuration = new Configuration();
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInitHDFS() {
        System.out.println(fileSystem);
    }

    /**
     * boolean delSrc 指是否将原文件删除
     * Path src 指要下载的文件路径
     * Path dst 指将文件下载到的路径
     * boolean useRawLocalFileSystem 是否开启文件效验
     */
    @Test
    public void testGet() {
        try {
            fileSystem.copyToLocalFile(true, new Path("/user/lanp/test/2.txt"),
                    new Path("D:\\workDir\\otherFile\\temp\\2019-1\\customer.txt"),
                    true);
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMkdir() {
        try {
            fileSystem.mkdirs(new Path("/user/lanp/test/gxp"));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件夹 ，如果是非空文件夹，参数2必须给值true
     */
    @Test
    public void testDeleteDir() {
        try {
            fileSystem.delete(new Path("/user/lanp/test/gxp"), false);
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testReNameDir() {
        try {
            fileSystem.rename(new Path("/user/lanp/test/a.txt"), new Path("/user/lanp/test/lp.txt"));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * boolean recursive 递归
     */
    @Test
    public void testShowFile() {
        try {
            RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/user/lanp"), true);

            while (listFiles.hasNext()) {
                LocatedFileStatus fileStatus = listFiles.next();
                System.out.println(fileStatus.getPath().getName());
                System.out.println(fileStatus.getBlockSize());
                System.out.println(fileStatus.getPermission());

                System.out.println(fileStatus.getLen());

                BlockLocation[] blockLocations = fileStatus.getBlockLocations();

                for (BlockLocation blockLocation : blockLocations) {
                    System.out.println("block-length:" + blockLocation.getLength() + "--" + "block-offset:" + blockLocation.getOffset());

                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts) {
                        System.out.println(host);
                    }
                }

                System.out.println("-------------------- 分割线 -------------------------");
            }

            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFindAtHDFS() {
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/user/lanp/test"));
            String flag = "-------- d --------    ";

            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    flag = "+++++++ f ++++++   ";
                }

                System.out.println(flag + fileStatus.getPath().getName());
            }


            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPutByStream() {
        try {
            /*FileInputStream inputStream = new FileInputStream(new File("D:\\workDir\\otherFile\\temp\\2019-1\\customer.txt"));
            FSDataOutputStream outputStream = fileSystem.create(new Path("/user/lanp/test/lanpeng.txt"));*/

            FSDataInputStream inputStream = fileSystem.open(new Path("/user/lanp/test/lanpeng.txt"));

            IOUtils.copyBytes(inputStream, System.out, 1024);
            IOUtils.closeStream(inputStream);
//            IOUtils.closeStream(outputStream);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
