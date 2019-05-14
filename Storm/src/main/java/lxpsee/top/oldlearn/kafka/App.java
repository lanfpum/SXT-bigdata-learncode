package lxpsee.top.oldlearn.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/14 17:20.
 */
public class App {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("ip201:2181");
        // BrokerHosts hosts, String topic, String zkRoot, String id
        SpoutConfig config = new SpoutConfig(hosts, "test-second-2019-3-12",
                "/test-second-2019-3-12", UUID.randomUUID().toString());
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout spout = new KafkaSpout(config);

        builder.setSpout("kafkaspout", spout).setNumTasks(2);
        builder.setBolt("split-bolt", new SplitBolt(), 2).shuffleGrouping("kafkaspout").setNumTasks(2);

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(true);

        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());
    }
}
