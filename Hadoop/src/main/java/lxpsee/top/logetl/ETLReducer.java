package lxpsee.top.logetl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/28 12:04.
 */
public class ETLReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, NullWritable.get());
        }

    }
}
