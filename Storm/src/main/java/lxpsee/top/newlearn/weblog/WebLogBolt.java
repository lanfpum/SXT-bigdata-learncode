package lxpsee.top.newlearn.weblog;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 22:23.
 */
public class WebLogBolt implements IRichBolt {
    private OutputCollector collector;
    private String valueString;
    private int num;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        try {
            valueString = input.getStringByField("log");

            if (null != valueString) {
                num++;
                System.err.println(Thread.currentThread().getName()
                        + " lines :" + num + " session_id: " + valueString.split("\t")[1]);
            }

            collector.ack(input);
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            collector.fail(input);
            e.printStackTrace();
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
