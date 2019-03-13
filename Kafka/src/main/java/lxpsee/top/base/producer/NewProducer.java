package lxpsee.top.base.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/12 23:50.
 * <p>
 * Kafka服务端的主机名和端口号
 * 等待所有副本节点的应答
 * 消息发送最大尝试次数
 * 一批消息处理大小
 * 请求延时
 * 发送缓存区内存大小
 * key序列化
 * value序列化
 */
public class NewProducer {
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

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<String, String>(
                    "test-first-2019-3-12", Integer.toString(i), "hello world from new " + i));
        }

        producer.close();
    }
}
