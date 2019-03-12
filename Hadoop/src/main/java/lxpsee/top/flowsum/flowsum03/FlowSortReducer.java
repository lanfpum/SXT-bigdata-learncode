package lxpsee.top.flowsum.flowsum03;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 23:10.
 */
public class FlowSortReducer extends Reducer<FlowSortBean, Text, Text, FlowSortBean> {

    @Override
    protected void reduce(FlowSortBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(), key);
    }
}
