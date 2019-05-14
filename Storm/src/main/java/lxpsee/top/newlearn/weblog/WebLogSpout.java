package lxpsee.top.newlearn.weblog;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 22:25.
 */
public class WebLogSpout implements IRichSpout {
    private BufferedReader bufferedReader;
    private SpoutOutputCollector collector;
    private String str;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        try {
            this.bufferedReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream("D:\\workDir\\otherFile\\temp\\2019-3\\weblog.txt"), "UTF-8"));
            this.collector = collector;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        try {
            while ((str = bufferedReader.readLine()) != null) {
                collector.emit(new Values(str));
                Thread.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
