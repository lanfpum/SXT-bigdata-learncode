package lxpsee.top.oldlearn.calllog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 15:45.
 */
public class CallLogCountBolt implements IRichBolt {
    private Map<String, Integer> callMap;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        callMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String call = input.getStringByField("call");
        Integer duration = input.getIntegerByField("duration");

        if (callMap.containsKey(call)) {
            callMap.put(call, callMap.get(call) + duration);
        } else {
            callMap.put(call, duration);
        }

        collector.ack(input);
    }

    public void cleanup() {
        for (Map.Entry<String, Integer> entry : callMap.entrySet()) {
            System.out.println(entry.getKey() + "---" + entry.getValue() / 60 + "分");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
