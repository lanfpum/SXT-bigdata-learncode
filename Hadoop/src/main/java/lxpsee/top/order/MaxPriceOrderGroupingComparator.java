package lxpsee.top.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/1/21 08:56.
 */
public class MaxPriceOrderGroupingComparator extends WritableComparator {

    public MaxPriceOrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) b;
        return abean.getOrderId().compareTo(bbean.getOrderId());
    }
}
