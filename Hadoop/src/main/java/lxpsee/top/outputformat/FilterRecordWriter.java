package lxpsee.top.outputformat;

import lxpsee.top.mrconstant.Constant;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 22:26.
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    private static final String LXP_SEE_TOP_OUTPUT = "/user/lanp/test/lxpsee.log";
    private static final String OTHER_OUTPUT       = "/user/lanp/test/other.log";

    FSDataOutputStream lxpseeOutput = null;
    FSDataOutputStream otherOutput  = null;
    FileSystem         fileSystem   = null;

    public FilterRecordWriter(TaskAttemptContext job) {
        try {
            // 获取文件系统，根据两个文件输出路径创建两个输出流
            fileSystem = FileSystem.get(job.getConfiguration());

            Path lxpseePath = new Path(LXP_SEE_TOP_OUTPUT);

            /*if (fileSystem.exists(lxpseePath)) {
                fileSystem.delete(lxpseePath, true);
            }*/

            Path otherPath = new Path(OTHER_OUTPUT);

          /*  if (fileSystem.exists(otherPath)) {
                fileSystem.delete(otherPath, true);
            }*/

            lxpseeOutput = fileSystem.create(lxpseePath);
            otherOutput = fileSystem.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 根据过滤条件写逻辑代码
        String keyLine = key.toString();

        if (keyLine.contains("lxpsee")) {
            lxpseeOutput.write(keyLine.getBytes());
        } else {
            otherOutput.write(keyLine.getBytes());
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (lxpseeOutput != null) {
            lxpseeOutput.close();
        }

        if (otherOutput != null) {
            otherOutput.close();
        }
    }
}
