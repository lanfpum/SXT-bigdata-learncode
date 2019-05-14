package lxpsee.top.oldlearn.ensure;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Random;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 17:00.
 */
public class WCBolt implements IRichBolt {
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String line = input.getStringByField("msg");

        if (new Random().nextBoolean()) {
            collector.ack(input);
            System.out.println(this + " : ack() : " + line + " : " + input.getMessageId().toString());
        } else {
            collector.fail(input);
            System.out.println(this + " : fail() : " + line + " : " + input.getMessageId().toString());
        }


    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
