package lxpsee.top.newlearn.weblog;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/3/13 22:54.
 * <p>
 * 创建拓扑对象
 * 设置Spout和bolt
 * 配置Worker开启个数
 */
public class WebLogMain {
    public static void main(String[] args) {
        TopologyBuilder top = new TopologyBuilder();
        top.setSpout("weblogspout", new WebLogSpout(), 1);
        top.setBolt("weblogbolt", new WebLogBolt(), 1).shuffleGrouping("weblogspout");

        Config config = new Config();
        config.setNumWorkers(4);

        if (args.length > 0) {
            try {
                // 分布式提交
                StormSubmitter.submitTopology(args[0], config, top.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
            // 本地提交
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("weblogtopology", config, top.createTopology());
        }


    }
}
