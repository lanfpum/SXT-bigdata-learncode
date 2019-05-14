package lxpsee.top.oldlearn.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 08:59.
 */
public class App {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("wcspout", new WCSpout(), 2).setNumTasks(2);
        builder.setBolt("splitBolt", new SplitBolt(), 4).shuffleGrouping("wcspout").setNumTasks(4);
        builder.setBolt("countBolt", new CountBolt(), 4).fieldsGrouping("splitBolt", new Fields("word")).setNumTasks(4);

        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);

        if (args.length > 0) {
            try {
                // 分布式提交
                StormSubmitter.submitTopology("wctop", config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 本地提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wc", config, builder.createTopology());
        }
    }
}
