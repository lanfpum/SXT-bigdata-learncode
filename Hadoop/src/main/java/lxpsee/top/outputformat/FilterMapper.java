package lxpsee.top.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 22:38.
 */
public class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        text.set(value.toString() + "\r\n");
        context.write(text, NullWritable.get());
    }
}
