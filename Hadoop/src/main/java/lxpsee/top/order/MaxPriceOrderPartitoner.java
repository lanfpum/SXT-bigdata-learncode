package lxpsee.top.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 08:54.
 */
public class MaxPriceOrderPartitoner extends Partitioner<OrderBean, NullWritable> {
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
