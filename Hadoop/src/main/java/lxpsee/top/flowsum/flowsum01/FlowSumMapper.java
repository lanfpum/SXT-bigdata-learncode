package lxpsee.top.flowsum.flowsum01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 22:14.
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();
    Text     text     = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phoneNum = fields[1];
        int len = fields.length;
        long upFlow = Long.parseLong(fields[len - 3]);
        long downFlow = Long.parseLong(fields[len - 2]);

        text.set(phoneNum);
        flowBean.set(upFlow, downFlow);

        context.write(text, flowBean);
    }
}
