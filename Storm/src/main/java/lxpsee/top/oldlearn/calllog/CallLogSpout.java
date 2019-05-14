package lxpsee.top.oldlearn.calllog;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 15:32.
 */
public class CallLogSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private Random random = new Random();
    private int idx = 0;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if (this.idx < 1000) {
            List<String> callNumList = new ArrayList<String>(4);
            callNumList.add("15012223333");
            callNumList.add("15014445555");
            callNumList.add("15016662222");
            callNumList.add("15018889999");

            int localIdx = 0;

            while (localIdx++ <= 100 && this.idx <= 10000) {
                String caller = callNumList.get(random.nextInt(4));
                String callee = callNumList.get(random.nextInt(4));

                while (caller == callee) {
                    callee = callNumList.get(random.nextInt(4));
                }

                Integer duration = random.nextInt(60);
                collector.emit(new Values(caller, callee, duration));
            }
        }
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("caller", "callee", "duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
