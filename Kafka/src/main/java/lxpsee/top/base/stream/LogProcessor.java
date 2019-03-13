package lxpsee.top.base.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 16:14.
 */
public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    public void process(byte[] key, byte[] value) {
        String input = new String(value);

        if (input.contains(">>>")) {
            input = input.split(">>>")[1].trim();
        }

        context.forward("logProcessor".getBytes(), input.getBytes());
    }

    public void punctuate(long l) {

    }

    public void close() {

    }
}
