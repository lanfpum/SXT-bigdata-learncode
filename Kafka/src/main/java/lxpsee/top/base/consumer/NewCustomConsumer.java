package lxpsee.top.base.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 09:34.
 * <p>
 * 定义kakfa 服务的地址，不需要将所有broker指定上
 * 制定consumer group
 * 是否自动确认offset
 * 自动确认offset的时间间隔
 * key的序列化类
 * value的序列化类
 */
public class NewCustomConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "ip202:9092");
        properties.put("group.id", "lanpeng-test2");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // 消费者订阅的topic, 可同时订阅多个
        consumer.subscribe(Arrays.asList("test-first-2019-3-12", "test-first-2019-3-13", "test-second-2019-3-12"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
