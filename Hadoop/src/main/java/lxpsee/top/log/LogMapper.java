package lxpsee.top.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/24 06:47.
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean result = processLog(line, context);

        if (!result) {
            return;
        }

        context.write(value, NullWritable.get());
    }

    private boolean processLog(String line, Context context) {
        String[] fields = line.split(" ");

        if (fields.length > 11) {
            context.getCounter("logMap", "true").increment(1);
            return true;
        } else {
            context.getCounter("logMap", "false").increment(1);
            return false;
        }
    }
}
