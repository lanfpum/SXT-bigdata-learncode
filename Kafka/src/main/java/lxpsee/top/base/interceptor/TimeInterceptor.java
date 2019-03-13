package lxpsee.top.base.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 10:49.
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(record.topic(),
                record.partition(), record.timestamp(), record.key(),
                System.currentTimeMillis() + " , " + record.value().toString());
    }

    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
