package lxpsee.top.flowsum.flowsum02;

import lxpsee.top.flowsum.flowsum01.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/19 22:46.
 */
public class FlowSumpartitioner extends Partitioner<Text, FlowBean> {

    public int getPartition(Text key, FlowBean flowBean, int numPartitions) {
        String prePhoneNum = key.toString().substring(0, 3);
        int partition = 4;

        if ("136".equals(prePhoneNum)) {
            partition = 0;
        } else if ("137".equals(prePhoneNum)) {
            partition = 1;
        } else if ("138".equals(prePhoneNum)) {
            partition = 2;
        } else if ("139".equals(prePhoneNum)) {
            partition = 3;
        }
        return partition;
    }
}
