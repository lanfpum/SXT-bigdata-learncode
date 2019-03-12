package lxpsee.top.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 18:30.
 * <p>
 * 注意BytesWritable
 */
public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration;

    private BytesWritable value = new BytesWritable();

    private boolean   processed = false;
    private FileSplit split;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    /**
     * 1.定义一个缓存
     * 2.获取文件系统
     * 3.读取内容
     * 3.1打开输入流
     * 3.2读取文件内容
     * 3.3输出 文件内容
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!processed) {
            int length = (int) split.getLength();
            byte[] contents = new byte[length];
            Path path = split.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream inputStream = null;

            try {
                inputStream = fileSystem.open(path);
                IOUtils.readFully(inputStream, contents, 0, length);
                value.set(contents, 0, length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                IOUtils.closeStream(inputStream);
            }

            processed = true;
            return true;
        }

        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
