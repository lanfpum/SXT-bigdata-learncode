package lxpsee.top.oldlearn.wordcount;

import lxpsee.top.oldlearn.utils.ShowDetailUtil;
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
 * Created by 努力常态化 on 2019/3/14 08:34.
 */
public class WCSpout implements IRichSpout {
    private SpoutOutputCollector collector;
    private List<String> stringList;
    private Random random = new Random();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        stringList = new ArrayList<String>(4);
        stringList.add("hello world little ge");
        stringList.add("hello world little ping");
        stringList.add("hello world little jim");
        stringList.add("hello world little curry");
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        String line = stringList.get(random.nextInt(4));
        ShowDetailUtil.sendTpClient(this, "nextTuple() " + line, "ip205", 7777);
        collector.emit(new Values(line));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
