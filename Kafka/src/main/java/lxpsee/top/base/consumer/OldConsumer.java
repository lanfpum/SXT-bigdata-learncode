package lxpsee.top.base.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 09:20.
 * <p>
 * 过时的API
 */
public class OldConsumer {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "ip201:2181,ip202:2181,ip203:2181");
        properties.put("group.id", "lanpeng-test2");
        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));

        HashMap<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put("test-first-2019-3-12", 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumerConnector.createMessageStreams(topicCount);
        KafkaStream<byte[], byte[]> stream = streams.get("test-first-2019-3-12").get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        while (iterator.hasNext()) {
            System.out.println(new String(iterator.next().message()));
        }
    }
}
