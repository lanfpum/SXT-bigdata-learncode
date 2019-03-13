package lxpsee.top.base.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 10:52.
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {
    private int errorCounter = 0;
    private int successCounter = 0;

    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // 统计成功和失败的次数

        if (exception == null) {
            successCounter++;
        } else {
            errorCounter++;
        }

    }

    public void close() {
        System.out.println("Successful sent:" + successCounter);
        System.out.println("Failed sent:" + errorCounter);
    }

    public void configure(Map<String, ?> configs) {

    }
}
