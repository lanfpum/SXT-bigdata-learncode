package lxpsee.top.base.producer;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/12 23:40.
 * <p>
 * 过时的生产者API
 */
public class OldProducer {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "ip202:9092,ip203:9092,ip204:9092");
        properties.put("request.required.acks", "1");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        Producer<Integer, String> producer = new Producer<Integer, String>(new ProducerConfig(properties));
        KeyedMessage<Integer, String> message = new KeyedMessage<Integer, String>("test-first-2019-3-12", "hello world");
        producer.send(message);
    }

}
