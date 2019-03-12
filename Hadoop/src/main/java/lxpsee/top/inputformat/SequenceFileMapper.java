package lxpsee.top.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 18:48.
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    private Text fileNameKey;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取切片信息,路径 ，根据切片路径获取文件名称
        Path path = ((FileSplit) context.getInputSplit()).getPath();
        fileNameKey = new Text(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(fileNameKey, value);
    }
}
