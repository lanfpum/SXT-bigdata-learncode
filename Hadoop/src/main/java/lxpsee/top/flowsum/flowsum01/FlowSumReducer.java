package lxpsee.top.flowsum.flowsum01;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 22:28.
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sum_upFlow = 0;
        long sum_downFlow = 0;

        for (FlowBean value : values) {
            sum_upFlow += value.getUpFlow();
            sum_downFlow += value.getDownFlow();
        }

        flowBean.set(sum_upFlow, sum_downFlow);
        context.write(key, flowBean);
    }
}
