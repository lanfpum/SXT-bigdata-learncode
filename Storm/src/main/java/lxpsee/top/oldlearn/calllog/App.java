package lxpsee.top.oldlearn.calllog;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 15:49.
 */
public class App {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("CLSpout", new CallLogSpout());
        builder.setBolt("clbolt1", new CallLogCreatorBolt()).shuffleGrouping("CLSpout");
        builder.setBolt("clbolt2", new CallLogCountBolt()).fieldsGrouping("clbolt1", new Fields("call"));

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("CallLogTopStorm", config, builder.createTopology());
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        cluster.shutdown();
    }
}
