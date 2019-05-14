package lxpsee.top.oldlearn.ensure;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 16:56.
 */
public class WCSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private List<String> states;
    private int index = 0;

    private Random random = new Random();
    private Map<Long, String> messages = new HashMap<Long, String>();
    private Map<Long, Integer> failMessages = new HashMap<Long, Integer>();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        states = new ArrayList<String>(4);
        states.add("hello little xx");
        states.add("baba love you");
        states.add("i hope you happy");
        states.add("i always love you");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if (index++ < 5) {
            String line = states.get(random.nextInt(4));
            long timestamp = System.currentTimeMillis();
            messages.put(timestamp, line);
            collector.emit(new Values(line), timestamp);
            System.out.println(this + "nextTuple() : " + line + " : " + timestamp);
        }
    }

    public void ack(Object msgId) {
        failMessages.remove(msgId);
        messages.remove(msgId);
    }

    public void fail(Object msgId) {
        Long ts = (Long) msgId;
        Integer retryNum = failMessages.get(ts);
        retryNum = (retryNum == null ? 0 : retryNum);

        if (retryNum > 3) {
            failMessages.remove(ts);
            messages.remove(ts);
        } else {
            collector.emit(new Values(messages.get(ts)), ts);
            System.out.println(this + "fail() : " + messages.get(ts) + " : " + ts);
            retryNum++;
            failMessages.put(ts, retryNum);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
