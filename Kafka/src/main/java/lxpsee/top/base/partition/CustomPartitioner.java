package lxpsee.top.base.partition;


import kafka.producer.Partitioner;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 08:48.
 * <p>
 * 过时的API
 */
public class CustomPartitioner implements Partitioner {

    public CustomPartitioner() {
        super();
    }

    public int partition(Object key, int numPartitions) {
        return 0;
    }
}
