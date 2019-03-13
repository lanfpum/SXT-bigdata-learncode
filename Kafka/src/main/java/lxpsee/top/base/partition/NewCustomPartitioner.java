package lxpsee.top.base.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 08:50.
 */
public class NewCustomPartitioner implements Partitioner {
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
