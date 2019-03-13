package lxpsee.top.base.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 08:51.
 */
public class PartitionerProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "ip202:9092,ip203:9092,ip204:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("partitioner.class", "lxpsee.top.base.partition.NewCustomPartitioner");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.send(new ProducerRecord<String, String>("test-first-2019-3-12", "001", "hello little xixix"));
    }
}
