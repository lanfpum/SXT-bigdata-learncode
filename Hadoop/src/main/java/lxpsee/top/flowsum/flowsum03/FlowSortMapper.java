package lxpsee.top.flowsum.flowsum03;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 23:05.
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, Text> {

    FlowSortBean bean = new FlowSortBean();
    Text         text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phoneNum = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        bean.set(upFlow, downFlow);
        text.set(phoneNum);

        context.write(bean, text);
    }
}
