package lxpsee.top.oldlearn.wordcount;

import lxpsee.top.oldlearn.utils.ShowDetailUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 08:53.
 */
public class CountBolt implements IRichBolt {
    private Map<String, Integer> wcMap;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        wcMap = new HashMap<String, Integer>();
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Integer count = input.getIntegerByField("count");
        ShowDetailUtil.sendTpClient(this, "execute() " + word + "--" + count, "ip205", 9999);

        if (!wcMap.containsKey(word)) {
            wcMap.put(word, 1);
        } else {
            wcMap.put(word, wcMap.get(word) + count);
        }

    }

    public void cleanup() {
        for (Map.Entry<String, Integer> entry : wcMap.entrySet()) {
            System.out.println(entry.getKey() + " ++++++ " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
